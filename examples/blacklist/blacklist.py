import daft

from frigg.common_crawl.extractors.image import ImageExtractor
from frigg.filters.blacklist import BlacklistFilter

url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-33/segments/1754151279521.11/wat/CC-MAIN-20250802220907-20250803010907-00000.warc.wat.gz"
df = daft.read_warc(url)

extractor = ImageExtractor()
blacklist = BlacklistFilter()

df = extractor(df)
df = blacklist(df)

df.show()
