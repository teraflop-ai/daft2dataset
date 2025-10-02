from typing import Optional

import daft
from daft import DataType, col
from fake_useragent import UserAgent
from urllib3 import PoolManager, ProxyManager, Retry, Timeout


@daft.udf(return_dtype=DataType.binary())
class ImageDownloaderUDF:
    def __init__(
        self, proxy: Optional[str] = None, timeout: float = 1.0, retries: int = 0
    ):
        if proxy:
            self.http = ProxyManager(proxy_url=proxy)
        else:
            self.http = PoolManager()
        self.user_agent = UserAgent().random
        self.timeout = timeout
        self.retries = retries

    def __call__(self, urls):
        return [self.download_image(url) for url in urls.to_pylist()]

    def download_image(self, url: str) -> bytes:
        try:
            request = self.http.request(
                "GET",
                url,
                headers={"User-Agent": self.user_agent},
                timeout=Timeout(self.timeout),
                retries=Retry(total=self.retries),
            )
            response = request.data
            return response
        except Exception as e:
            return None


class ImageDownloader:
    def __init__(
        self,
        input_column: str = "url",
        output_column: str = "image_bytes",
        decode: bool = True,
        decode_error: str = "null",
        timeout: float = 1.0,
        retries: int = 0,
        proxy: Optional[str] = None,
    ):
        self.input_column = input_column
        self.output_column = output_column
        self.decode = decode
        self.decode_error = decode_error
        self.timeout = timeout
        self.retries = retries
        self.proxy = proxy

    def __call__(self, df):
        df = df.with_column(
            self.output_column,
            ImageDownloaderUDF.with_init_args(
                proxy=self.proxy, timeout=self.timeout, retries=self.retries
            )(col(self.input_column)),
        )
        if self.decode:
            df = df.with_column(
                "image", col(self.output_column).image.decode(self.decode_error)
            )
        return df
