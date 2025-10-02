import daft
from daft import col

from frigg.common_crawl.downloaders.image import ImageDownloader

df = daft.from_pydict(
    {
        "urls": [
            "https://raw.githubusercontent.com/yavuzceliker/sample-images/main/docs/image-1.jpg"
        ],
    }
)

downloader = ImageDownloader()

df = downloader(df)
df.show()
