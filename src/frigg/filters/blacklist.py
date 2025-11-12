from urllib.parse import urlparse

import daft
from daft import col

KEYWORD_BLACKLIST = ["xxx", "porn", "sex", "adult", "kif.tube"]

@daft.udf(return_dtype=daft.DataType.bool())
def has_blacklisted_keyword(url: str) -> bool:
    if not url:
        return False
    url = url.lower()
    return any(word in url for word in KEYWORD_BLACKLIST)

@daft.func(return_dtype=daft.DataType.string())
def extract_domain(url: str) -> str:
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower().lstrip("www.")
    except Exception:
        return None


class BlacklistFilter:
    def __init__(self, input_column: str = "url"):
        self.input_column = input_column

    def __call__(self, df):
        blacklist_df = daft.read_huggingface("TeraflopAI/url-blacklist")
        blacklist_df = blacklist_df.with_column(
            "netloc", col("url_blacklist").str.lower()
        )

        df = df.with_column("netloc", extract_domain(col(self.input_column)))

        df = df.join(
            blacklist_df.select(col("netloc")), on="netloc", how="anti"
        ).exclude("netloc")

        return df
