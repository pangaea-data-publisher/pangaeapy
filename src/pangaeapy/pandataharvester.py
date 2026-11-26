import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse
import zipfile

import aiohttp
import requests

class PanDataHarvester:
    """
    Downloads binary data from the PANGAEA tape archive.

    Parameters
    ----------
    dataset: PanDataSet
        The dataset, which initiates the PanDataHarvester.


    This class bundles the download functionality of pangaeapy.
    When initiated via PanDataSet.download(), the selected files are downloaded asynchronously.
    They are stored in the local cache in their original file format and under their original name.
    The Harvester will check if the file already exists before downloading.

    """

    def __init__(self, dataset):
        self.id = dataset.id
        self.auth_token = dataset.auth_token
        self.data = dataset.data
        self.cachedir = dataset.cachedir
        self.cachedir.mkdir(parents=True, exist_ok=True)
        self.columns = dataset.columns  # list of column names
        self.data_index = dataset.data_index
        self.semaphore = asyncio.Semaphore(5)  # Limit concurrent downloads
        self.timeout = aiohttp.ClientTimeout(total=dataset.timeout)  # set connection timeout


    def _list_available_data(self):
        """List available binary data (filenames) in the dataset.

        Returns
        -------
            List of filenames
        """
        if self.data_index:
            available_data = (self.data
                              .iloc[self.data_index][self.columns]  # select rows and columns
                              .to_numpy().flatten().tolist())  # extract values and reduce dimensions
        else:
            # If no index is supplied return all rows
            available_data = self.data[self.columns].to_numpy().flatten().tolist()
        return available_data

    async def _download_file(self, session, url, filename, max_retries=4):
        """Download a single file asynchronously, handling 503 errors."""
        filepath = Path(self.cachedir, filename)

        attempt = 0
        async with self.semaphore:
            while attempt < max_retries:
                try:
                    async with session.get(url) as response:
                        size = int(response.headers.get('content-length', 0))
                        if filepath.exists() and (filepath.stat().st_size >= size):
                            print(f"File {filename} already exists, skipping.")
                            return filepath

                        if response.status == 429:
                            wait_time = response.headers.get('retry-after', 0)
                            wait_time = int(wait_time) if wait_time != 0 else 10
                            print(f"Got response status 429 (Too many connections) while trying to download {filename}.\n"
                                  f" Retrying in {wait_time} seconds.")
                            await asyncio.sleep(wait_time)
                            attempt += 1
                            continue

                        if response.status == 503:
                            sleep = 30 * (attempt + 1)
                            print(f"{filename} is being retrieved from tape. Retrying in {sleep} seconds...")
                            await asyncio.sleep(sleep)  # Wait 2 minutes before retrying
                            attempt += 1
                            continue

                        with open(filepath, "wb") as f:
                            async for chunk in response.content.iter_any():
                                f.write(chunk)

                    print(f"Downloaded {filename} successfully!")
                    return filepath

                except aiohttp.ClientResponseError as e:
                    attempt += 1
                    if attempt == max_retries:
                        raise e
                    print(f"Error {e.status} encountered. Retrying ({attempt}/{max_retries})...")

            print(f"Exceeded maximum number of retries ({max_retries}). Cancel download.")
            return None


    async def download_files(self):
        """Download all binary files asynchronously."""
        binary_files = self._list_available_data()
        dataset_id = self.id

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            session.headers.update({"Authorization": f"Bearer {self.auth_token}"})
            tasks = []
            for filename in binary_files:
                if filename.startswith("https:"):
                    url = filename
                    parsed_url = urlparse(url)
                    # extract filename from url and decode special characters
                    filename = PurePosixPath(unquote(parsed_url.path)).name
                else:
                    url = f"https://download.pangaea.de/dataset/{dataset_id}/files/{filename}"

                tasks.append(self._download_file(session, url, filename))

            results = await asyncio.gather(*tasks)
            downloaded_files = [result for result in results if result]

        return downloaded_files


    def run_download(self):
        """Start asynchronous file download for single file downloads or download
         the zip file and extract its contents if the whole data set is requested.
         This requires a valid auth_token (also called Bearer Token).

         Returns
         -------
            List of downloaded files
         """
        if (self.data_index == []) and (all(["URL" not in column for column in self.columns])):
            # User wants the whole binary data set
            # Download the data set via the ZIP link, which requires a valid auth_token
            # Data sets with a URL binary column do not have a zip download available
            downloaded_files = self.download_zip_file()
        else:
            with ThreadPoolExecutor(max_workers=1) as pool:
                downloaded_files = pool.submit(asyncio.run, self.download_files()).result()
        return downloaded_files


    def download_zip_file(self):
        """Download a complete binary data set via the ZIP link.
        Requires a valid auth_token (also called Bearer Token), which can be found at https://www.pangaea.de/user/.
        """
        url = f"https://download.pangaea.de/dataset/{self.id}/allfiles.zip"
        zip_path = Path(self.cachedir, "allfiles.zip")
        extract_dir = Path(self.cachedir)
        url_headers = {
            "Authorization": f"Bearer {self.auth_token}",
        }
        try:
            with requests.get(url, stream=True, headers=url_headers) as r:
                if r.status_code == 401:
                    print(
                        "401 Client Error: Unauthorized access.\n"
                        "Please provide a valid auth_token when opening the data set.\n"
                        "You can find your auth_token (Bearer Token) on your PANGAEA user page at https://www.pangaea.de/user/"
                    )
                    return []
                r.raise_for_status()

                # Stream download to disk
                with open(zip_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            # Extract and collect filenames
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                downloaded_files = [Path(self.cachedir, f) for f in zip_file.namelist()]
                zip_file.extractall(extract_dir)

            return downloaded_files

        except requests.exceptions.RequestException as e:
            print(f"Download failed: {e}")
            return []
        except zipfile.BadZipFile as e:
            print(f"Extraction failed: {e}")
            return []
        finally:
            if zip_path.exists():
                try:
                    zip_path.unlink()
                except Exception as e:
                    print(f"Failed to delete ZIP file: {e}")
