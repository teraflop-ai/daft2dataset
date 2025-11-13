import daft

from frigg.common_crawl.downloaders.video import VideoDownloader

df = daft.from_pydict(
    {
        "url": [
            "https://www.professionalmuscle.com/forums/styles/default/xenforo/add_to_home.mp4"
        ],
    }
)

downloader = VideoDownloader()

df = downloader(df)
df.show()
