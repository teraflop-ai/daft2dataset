import os
import uuid
from typing import Optional

import daft
from daft import DataType, col
from fake_useragent import UserAgent
from urllib3 import PoolManager, ProxyManager, Retry, Timeout


@daft.udf(return_dtype=DataType.string())
class VideoDownloaderUDF:
    def __init__(
        self,
        download_dir: str = "/tmp/video_downloads",
        proxy: Optional[str] = None,
        timeout: float = 1.0,
        retries: int = 0,
    ):
        os.makedirs(download_dir, exist_ok=True)
        self.download_dir = download_dir

        if proxy:
            self.http = ProxyManager(proxy_url=proxy)
        else:
            self.http = PoolManager()

        self.user_agent = UserAgent().random
        self.timeout = timeout
        self.retries = retries

    def __call__(self, urls):
        return [self.download_video(url) for url in urls.to_pylist()]

    def download_video(self, url: str) -> Optional[str]:
        try:
            response = self.http.request(
                "GET",
                url,
                headers={"User-Agent": self.user_agent},
                timeout=Timeout(self.timeout),
                retries=Retry(total=self.retries),
            )

            if response.status != 200:
                return None

            ext = os.path.splitext(url.split("?")[0])[-1]
            if len(ext) > 5 or not ext:
                ext = ".mp4"
            filename = f"{uuid.uuid4().hex}{ext}"
            filepath = os.path.join(self.download_dir, filename)

            with open(filepath, "wb") as fout:
                fout.write(response.data)

            return filepath
        except Exception:
            return None


class VideoDownloader:
    def __init__(
        self,
        input_column: str = "url",
        output_column: str = "video_path",
        timeout: float = 1.0,
        retries: int = 0,
        proxy: Optional[str] = None,
        download_dir: str = "/tmp/video_downloads",
    ):
        self.input_column = input_column
        self.output_column = output_column
        self.timeout = timeout
        self.retries = retries
        self.proxy = proxy
        self.download_dir = download_dir

    def __call__(self, df):
        df = df.with_column(
            self.output_column,
            VideoDownloaderUDF.with_init_args(
                download_dir=self.download_dir,
                proxy=self.proxy,
                timeout=self.timeout,
                retries=self.retries,
            )(col(self.input_column)),
        )
        return df
