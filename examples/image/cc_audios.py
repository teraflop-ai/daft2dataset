import daft

from frigg.common_crawl.audio import AudioExtractor

url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-33/segments/1754151279521.11/wat/CC-MAIN-20250802220907-20250803010907-00000.warc.wat.gz"
df = daft.read_warc(url)
extractor = AudioExtractor()
df = extractor(df)
df.show()
