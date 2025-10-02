from typing import Optional

import daft
import urllib3
from daft import DataType, col
from fake_useragent import UserAgent


@daft.udf(return_dtype=DataType.binary())
class ImageDownloaderUDF:
    def __init__(self, proxy: Optional[str] = None):
        if proxy:
            self.http = urllib3.ProxyManager(proxy)
        else:
            self.http = urllib3.PoolManager()
        self.user_agent = UserAgent().random

    def __call__(self, urls):
        return [self.download_image(url) for url in urls.to_pylist()]

    def download_image(self, url: str) -> bytes:
        request = self.http.request("GET", url, headers={"User-Agent": self.user_agent})
        response = request.data
        return response


class ImageDownloader:
    def __init__(
        self,
        input_column: str = "urls",
        output_column: str = "image_bytes",
        decode: bool = True,
        proxy: Optional[str] = None,
    ):
        self.input_column = input_column
        self.output_column = output_column
        self.decode = decode
        self.proxy = proxy

    def __call__(self, df):
        df = df.with_column(
            self.output_column,
            ImageDownloaderUDF.with_init_args(proxy=self.proxy)(col(self.input_column)),
        )
        if self.decode:
            df = df.with_column("image", col(self.output_column).image.decode())
        return df
