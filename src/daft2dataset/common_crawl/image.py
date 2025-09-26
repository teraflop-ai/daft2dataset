import hashlib
from urllib.parse import urljoin

import daft
import simdjson
from daft import col

url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-33/segments/1754151279521.11/wat/CC-MAIN-20250802220907-20250803010907-00000.warc.wat.gz"
df = daft.read_warc(url)
df = df.where(col("WARC-Type") == "metadata")
df = df.exclude(
    "Content-Length",
    "warc_headers",
    "WARC-Identified-Payload-Type",
    "WARC-Date",
    "WARC-Type",
)

def valid_image_link(link):
    valid_path = link.get("path", "") == "IMG@/src"
    valid_alt = len(link.get("alt", "")) > 0
    return valid_path and valid_alt


def extract_image_from_links(links):
    """Extract image from links"""
    filtered_links = [
        {"url": link["url"], "alt": link["alt"]}
        for link in links
        if valid_image_link(link)
    ]
    return filtered_links


def make_link_absolute(url, base_url):
    if url.startswith("http://") or url.startswith("https://"):
        return url
    try:
        return urljoin(base_url, url)
    except ValueError:
        return url


def make_links_absolute(links, base_url):
    return [
        {"url": make_link_absolute(link["url"], base_url), "alt": link["alt"]}
        for link in links
    ]


def parse_content(record):
    all_links = []
    record_data = simdjson.loads(record)
    envelope = record_data["Envelope"]
    payload = envelope["Payload-Metadata"]
    if "HTTP-Response-Metadata" not in payload:
        return
    http_resp = payload["HTTP-Response-Metadata"]
    if "HTML-Metadata" not in http_resp:
        return
    metadata = http_resp["HTML-Metadata"]
    if "Links" not in metadata:
        return
    links = metadata["Links"]
    base_url = envelope["WARC-Header-Metadata"]["WARC-Target-URI"]
    filtered_links = extract_image_from_links(links)
    filtered_links = make_links_absolute(filtered_links, base_url)
    filtered_links = [
        link
        for link in filtered_links
        if link["url"].startswith("http://") or link["url"].startswith("https://")
    ]
    for link in filtered_links:
        link["uid"] = str(hashlib.md5((link["alt"] + link["url"]).encode()).hexdigest())
    all_links.extend(filtered_links)
    return all_links


df = df.with_column(
    "links",
    col("warc_content").apply(
        parse_content,
        return_dtype=daft.DataType.list(
            daft.DataType.struct(
                {
                    "url": daft.DataType.string(),
                    "alt": daft.DataType.string(),
                    "uid": daft.DataType.string(),
                }
            )
        ),
    ),
)
df = df.exclude("warc_content")
df = df.explode("links")
df = df.with_columns(
    {
        "url": col("links")["url"],
        "alt": col("links")["alt"],
        "uid": col("links")["uid"],
    }
)
df = df.where(col("links").not_null())
df = df.exclude("links")
df = df.drop_duplicates("uid")
df.collect()
print(df.count_rows())