import daft

from frigg.twitch.download import TwitchDownloader

df = daft.from_pydict(
    {
        "url": [
            "https://www.twitch.tv/videos/1796140156"
        ],
    }
)

downloader = TwitchDownloader()

df = downloader(df)
df.show()