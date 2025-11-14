import daft
from daft import DataType, col
import streamlink
import subprocess
import json

@daft.udf(return_dtype=DataType.struct(
    {
        'id': DataType.string(), 
        'author': DataType.string(), 
        'category': DataType.string(), 
        'title': DataType.string(),
    }
))
class TwitchDownloaderUDF:
    def __init__(self):
        pass

    def __call__(self, urls):
        return [self.download_stream(url) for url in urls.to_pylist()]

    def extract_metadata(self, url: str):
        meta = json.loads(
            subprocess.check_output(["streamlink", "--json", url, "best"])
        )["metadata"]
        return meta

    def download_stream(self, url: str):
        session = streamlink.session.Streamlink()
        session.set_option("disable-ads", "true")
        session.set_option("disable-reruns", "true")
        session.set_option("low-latency", "true")
        
        info = self.extract_metadata(url)

        outfile = f"{info["title"]}.mp4"

        streams = session.streams(url)
        fd = streams["best"].open()
        with open(outfile, "wb") as f:
            for chunk in iter(lambda: fd.read(1024 * 1024), b""):
                f.write(chunk)
        fd.close()

        return info

class TwitchDownloader:
    def __init__(
        self,
        input_column: str = "url",
        output_column: str = "twitch_metadata",
    ):
        self.input_column = input_column
        self.output_column = output_column

    def __call__(self, df):
        df = df.with_column(
            self.output_column,
            TwitchDownloaderUDF.with_init_args()(col(self.input_column)),
        )
        return df
