# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2025 Bardia Moshiri <bardia@furilabs.com>
# Copyright (C) 2025 Luis Garcia <git@luigi311.com>

import asyncio
import aiohttp
import msgspec
import json
import os

from dbus_fast.aio import MessageBus
from dbus_fast.service import ServiceInterface, method, signal
from dbus_fast import BusType, Variant
from loguru import logger

from common.utils import download_file
from android_store.database import (
    init_database, save_packages_to_db, ensure_populated,
    search_packages, get_package_by_id
)
from android_store.api import (
    download_index, process_indexes, read_repo_list,
)
from android_store.andromeda import (
    ping_session_manager, install_app, remove_app,
    get_apps_info, compare_installed_with_repo
)

DEFAULT_REPO_CONFIG_DIR = "/usr/lib/store-provider/android_store/repos"
CUSTOM_REPO_CONFIG_DIR = "/etc/store-provider/android-store/repos"
DATABASE = os.path.expanduser("~/.cache/store-provider/android-store/android-store.db")
CACHE_DIR = os.path.expanduser("~/.cache/store-provider/android-store/repo")
DOWNLOAD_CACHE_DIR = os.path.expanduser("~/.cache/store-provider/android-store/downloads")

class FDroidInterface(ServiceInterface):
    def __init__(self, idle_callback=None):
        logger.info("Initializing F-Droid store daemon")
        super().__init__('io.FuriOS.AndroidStore.fdroid')
        self.session = None
        self.db = None
        self.json_enc = msgspec.json.Encoder()

        self.idle_callback = idle_callback
        self.idle_timer = None

        # Task queue implementation
        self._task_queue = asyncio.Queue()
        self._task_processor = None
        self._running = False

        # Start the task processor
        self._start_task_processor()

        # Start the idle timer
        self._reset_idle_timer()

    async def init_db(self):
        """Initialize the database"""
        os.makedirs(CACHE_DIR, exist_ok=True)
        os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)

        self.db = await init_database(DATABASE)

    async def ensure_session(self):
        """Ensure HTTP session exists"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def cleanup_session(self):
        """Clean up HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None

    def _start_task_processor(self):
        """Start the async task processor if it's not already running"""
        if not self._running:
            self._running = True
            self._task_processor = asyncio.create_task(self._process_task_queue())
            logger.info("Task processor started")

    def _reset_idle_timer(self):
        """Reset the idle timer when activity occurs"""
        if self.idle_callback:
            asyncio.create_task(self.idle_callback())

    async def _process_task_queue(self):
        """Process tasks in queue one at a time"""
        while self._running:
            try:
                # Get next task from queue
                task, future = await self._task_queue.get()

                # Reset idle timer on activity
                self._reset_idle_timer()

                try:
                    # Execute the task
                    result = await task()

                    # Set the result for the waiting caller
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                    logger.error(f"Task error: {e}")

                # Mark task as done
                self._task_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Task processor error: {e}")

    async def _queue_task(self, task_func):
        """Queue a task and wait for its result"""
        future = asyncio.Future()
        await self._task_queue.put((task_func, future))

        # Reset idle timer on activity
        self._reset_idle_timer()

        # Wait for the task to complete and return its result
        return await future

    async def process_repo_file(self, config_file, repo_dir):
        """
        Process a single repository configuration file by iterating through its mirrors sequentially.
        """
        repos = read_repo_list(config_file, repo_dir)
        repo_success = False

        for repo_url in repos:
            logger.info(f"Downloading {config_file} index from {repo_url} (from {repo_dir})")
            if await download_index(self.session, repo_url, config_file, CACHE_DIR):
                logger.info(f"Successfully downloaded {config_file}")
                repo_success = True
                break
            else:
                logger.error(f"Failed to download from {repo_url}, trying next mirror...")

        if not repo_success:
            logger.error(f"Failed to download {config_file} from all mirrors")

        return repo_success

    async def update_cache(self):
        await self.ensure_session()
        all_repo_files = set()

        if os.path.exists(CUSTOM_REPO_CONFIG_DIR) and os.path.isdir(CUSTOM_REPO_CONFIG_DIR):
            for config_file in os.listdir(CUSTOM_REPO_CONFIG_DIR):
                if os.path.isfile(os.path.join(CUSTOM_REPO_CONFIG_DIR, config_file)):
                    all_repo_files.add(config_file)
                    logger.info(f"Found repository in custom dir: {config_file}")

        if os.path.exists(DEFAULT_REPO_CONFIG_DIR) and os.path.isdir(DEFAULT_REPO_CONFIG_DIR):
            for config_file in os.listdir(DEFAULT_REPO_CONFIG_DIR):
                if os.path.isfile(os.path.join(DEFAULT_REPO_CONFIG_DIR, config_file)) and config_file not in all_repo_files:
                    all_repo_files.add(config_file)
                    logger.info(f"Found repository in default dir: {config_file}")

        tasks = []
        for config_file in all_repo_files:
            # Check custom dir first, then fall back to default
            if os.path.exists(os.path.join(CUSTOM_REPO_CONFIG_DIR, config_file)):
                repo_dir = CUSTOM_REPO_CONFIG_DIR
            else:
                repo_dir = DEFAULT_REPO_CONFIG_DIR

            tasks.append(asyncio.create_task(self.process_repo_file(config_file, repo_dir)))

        results = await asyncio.gather(*tasks)
        overall_success = any(results)

        packages = await process_indexes(CACHE_DIR, self.json_enc)
        await save_packages_to_db(self.db, packages)

        await self.cleanup_session()
        return overall_success

    async def ensure_populated(self):
        return await ensure_populated(self.db, self.update_cache)

    async def get_upgradable_packages(self):
        return await compare_installed_with_repo(self.db, msgspec.json.decode)

    @method()
    async def Search(self, query: 's') -> 's':
        async def _search_task():
            logger.info(f"Searching for {query}")
            results = []

            if not await ping_session_manager():
                return json.dumps(results)

            if not await self.ensure_populated():
                return json.dumps(results)

            results = await search_packages(self.db, query, msgspec.json.decode)
            return json.dumps(results)
        return await _search_task()

    @method()
    async def UpdateCache(self) -> 'b':
        async def _update_cache_task():
            if not await ping_session_manager():
                return False
            return await self.update_cache()
        return await self._queue_task(_update_cache_task)

    @method()
    async def Install(self, package_id: 's') -> 'b':
        async def _install_task():
            logger.info(f"Installing package {package_id}")

            if not await ping_session_manager():
                return False

            if not await self.ensure_populated():
                return False

            try:
                package_info = await get_package_by_id(self.db, package_id, msgspec.json.decode)
                if not package_info:
                    logger.error(f"Package {package_id} not found")
                    return False

                os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)
                await self.ensure_session()

                self.InstallStatus(package_id, "downloading")
                filepath = os.path.join(DOWNLOAD_CACHE_DIR, package_info['apk_name'])
                result = await download_file(
                    self.session, package_info['download_url'], filepath,
                    progress_callback=lambda p: self.DownloadProgress(package_id, p)
                )

                if not result:
                    return False

                logger.info(f"APK downloaded to: {filepath}")
                self.InstallStatus(package_id, "installing")
                success = await install_app(filepath)
                os.remove(filepath)

                if success:
                    self.AppInstalled(package_id)
                    logger.success(f"Successfully installed {package_id}")
                    return True
                logger.error(f"Failed to install {package_id}")
                return False
            except Exception as e:
                logger.error(f"Installation failed: {e}")
                return False
        return await self._queue_task(_install_task)

    @signal()
    def AppInstalled(self, package_id: 's') -> 's':
        return package_id

    @signal()
    def DownloadProgress(self, package_id: 's', progress: 'i') -> 'si':
        return package_id, progress

    @signal()
    def InstallStatus(self, package_id: 's', status: 's') -> 'ss':
        return package_id, status

    @method()
    async def GetRepositories(self) -> 'a(ss)':
        async def _get_repositories_task():
            logger.info("Getting repositories")
            repositories = []

            if not await ping_session_manager():
                return repositories

            repo_files = {}  # filename -> (repo_dir, url)

            if os.path.exists(CUSTOM_REPO_CONFIG_DIR) and os.path.isdir(CUSTOM_REPO_CONFIG_DIR):
                for repo_file in os.listdir(CUSTOM_REPO_CONFIG_DIR):
                    repo_path = os.path.join(CUSTOM_REPO_CONFIG_DIR, repo_file)
                    if os.path.isfile(repo_path):
                        with open(repo_path, 'r', encoding='utf-8') as f:
                            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                            if lines:
                                repo_files[repo_file] = (CUSTOM_REPO_CONFIG_DIR, lines[0])

            if os.path.exists(DEFAULT_REPO_CONFIG_DIR) and os.path.isdir(DEFAULT_REPO_CONFIG_DIR):
                for repo_file in os.listdir(DEFAULT_REPO_CONFIG_DIR):
                    if repo_file in repo_files:
                        continue

                    repo_path = os.path.join(DEFAULT_REPO_CONFIG_DIR, repo_file)
                    if os.path.isfile(repo_path):
                        with open(repo_path, 'r', encoding='utf-8') as f:
                            lines = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                            if lines:
                                repo_files[repo_file] = (DEFAULT_REPO_CONFIG_DIR, lines[0])

            for repo_file, (repo_dir, repo_url) in repo_files.items():
                source = "custom" if repo_dir == CUSTOM_REPO_CONFIG_DIR else "default"
                repositories.append([f"{repo_file} ({source})", repo_url])
            return repositories
        return await _get_repositories_task()

    @method()
    async def GetUpgradable(self) -> 'aa{sv}':
        async def _get_upgradable_task():
            logger.info("Getting upgradable")
            upgradable = []

            if not await ping_session_manager():
                return upgradable

            raw_upgradable = await self.get_upgradable_packages()
            for pkg in raw_upgradable:
                upgradable_info = {
                    'id': Variant('s', pkg['id']),
                    'name': Variant('s', pkg.get('name', pkg['id'])),
                    'packageName': Variant('s', pkg['id']),
                    'currentVersion': Variant('s', pkg['current_version']),
                    'availableVersion': Variant('s', pkg['available_version']),
                    'repository': Variant('s', pkg['repo_url']),
                    'package': Variant('s', json.dumps(pkg['packageInfo']))
                }
                upgradable.append(upgradable_info)
                logger.info(f"{upgradable_info['packageName'].value} {upgradable_info['name'].value} {upgradable_info['currentVersion'].value} {upgradable_info['availableVersion'].value}")
            return upgradable
        return await _get_upgradable_task()

    @method()
    async def UpgradePackages(self, packages: 'as') -> 'b':
        async def _upgrade_packages_task():
            logger.info(f"Upgrading packages {packages}")

            if not await ping_session_manager():
                return False

            upgradables = await self.get_upgradable_packages()
            upgrade_list = packages

            if not upgrade_list:
                upgrade_list = [pkg['id'] for pkg in upgradables]
                logger.info(f"Upgrading all available packages: {upgrade_list}")

            if not upgrade_list:
                logger.info("No packages to upgrade")
                return True

            os.makedirs(DOWNLOAD_CACHE_DIR, exist_ok=True)
            await self.ensure_session()

            for package_id in upgrade_list:
                for pkg in upgradables:
                    if pkg['id'] == package_id:
                        logger.info(f"Installing upgrade for {package_id}")
                        try:
                            package_info = pkg['packageInfo']
                            download_url = package_info['download_url']
                            apk_name = package_info['apk_name']
                            filepath = os.path.join(DOWNLOAD_CACHE_DIR, apk_name)

                            self.InstallStatus(package_id, "downloading")
                            result = await download_file(
                                self.session, download_url, filepath,
                                progress_callback=lambda p: self.DownloadProgress(package_id, p)
                            )
                            if not result:
                                logger.error(f"Failed to download {package_id}")
                                continue

                            logger.info(f"APK downloaded to: {filepath}")
                            self.InstallStatus(package_id, "installing")
                            success = await install_app(filepath)
                            os.remove(filepath)

                            if not success:
                                logger.error(f"Failed to upgrade {package_id}")
                                return False

                            break
                        except Exception as e:
                            logger.error(f"Error upgrading {package_id}: {e}")
                            return False
            await self.cleanup_session()
            return True
        return await self._queue_task(_upgrade_packages_task)

    @method()
    async def RemoveRepository(self, repo_id: 's') -> 'b':
        async def _remove_repository_task():
            logger.info(f"Removing repository {repo_id}")

            if not await ping_session_manager():
                return False
            return True
        return await self._queue_task(_remove_repository_task)

    @method()
    async def GetInstalledApps(self) -> 'aa{sv}':
        async def _get_installed_apps_task():
            logger.info("Getting installed apps")

            if not await ping_session_manager():
                return []
            return await get_apps_info()
        return await _get_installed_apps_task()

    @method()
    async def UninstallApp(self, package_name: 's') -> 'b':
        async def _uninstall_app_task():
            logger.info(f"Uninstalling app {package_name}")

            if not await ping_session_manager():
                return False
            return await remove_app(package_name)
        return await self._queue_task(_uninstall_app_task)

    async def cleanup(self):
        """Clean up resources when service is stopping"""
        self._running = False
        if self.idle_timer:
            self.idle_timer.cancel()
        if self._task_processor:
            self._task_processor.cancel()
            try:
                await self._task_processor
            except asyncio.CancelledError:
                pass
        await self.cleanup_session()
        if self.db:
            await self.db.close()

class AndroidStoreService:
    def __init__(self, idle_callback=None):
        logger.info("Initializing Android store service")
        self.bus = None
        self.fdroid_interface = None
        self.idle_callback = idle_callback

    async def setup(self):
        """Set up the D-Bus service"""
        self.bus = await MessageBus(bus_type=BusType.SESSION).connect()

        self.fdroid_interface = FDroidInterface(
            idle_callback=self.idle_callback
        )

        # Initialize the database
        await self.fdroid_interface.init_db()
        self.bus.export('/fdroid', self.fdroid_interface)
        await self.bus.request_name('io.FuriOS.AndroidStore')

        return self.bus

    async def cleanup(self):
        """Clean up resources"""
        if self.fdroid_interface:
            await self.fdroid_interface.cleanup()
