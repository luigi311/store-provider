# SPDX-License-Identifier: GPL-2.0-only
# Copyright (C) 2025 Bardia Moshiri <bardia@furilabs.com>
# Copyright (C) 2025 Luis Garcia <git@luigi311.com>

import os

from loguru import logger


async def download_file(
    session, url, output_path, headers=None, progress_callback=None
):
    """
    Download a file from a URL to the specified path.

    Args:
        session: aiohttp ClientSession
        url: URL to download from
        output_path: Path to save the file
        headers: Optional dict of HTTP headers to include in the request

    Returns:
        True if download was successful, False otherwise
    """
    try:
        # choose whether to pass headers
        if headers is not None:
            req = session.get(url, headers=headers)
        else:
            req = session.get(url)

        async with req as response:
            if response.status != 200:
                logger.error(f"Error downloading file: HTTP {response.status}")
                return False

            # Download the file in chunks
            with open(output_path, "wb") as f:
                total = int(response.headers.get("content-length", 0))
                downloaded = 0
                chunk_size = 65536
                last_progress = -1

                async for chunk in response.content.iter_chunked(chunk_size):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        progress = int(downloaded * 100 / total)
                        logger.trace(f"Download progress: {progress}%")
                        if progress_callback and progress != last_progress:
                            progress_callback(progress)
                            last_progress = progress
            return True
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return False
