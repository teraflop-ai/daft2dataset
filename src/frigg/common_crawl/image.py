from urllib.parse import urljoin

import daft
import simdjson
import xxhash
from daft import col


class Images:
    def __init__(self):
        pass

    def __call__(self, url):
        df = daft.read_warc(url)
        df = df.where(col("WARC-Type") == "metadata")
        df = df.exclude(
            "Content-Length",
            "warc_headers",
            "WARC-Identified-Payload-Type",
            "WARC-Date",
            "WARC-Type",
        )
        df = df.with_column(
            "links",
            col("warc_content").apply(
                self.parse_content,
                return_dtype=daft.DataType.list(
                    daft.DataType.struct(
                        {
                            "url": daft.DataType.string(),
                            "alt": daft.DataType.string(),
                            "hash": daft.DataType.string(),
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
                "hash": col("links")["hash"],
            }
        )
        df = df.where(col("links").not_null())
        df = df.exclude("links")
        df = df.drop_duplicates("hash")
        return df

    def valid_image_link(self, link):
        valid_path = link.get("path", "") == "IMG@/src"
        valid_alt = len(link.get("alt", "")) > 0
        return valid_path and valid_alt

    def extract_image_from_links(self, links):
        """Extract image from links"""
        filtered_links = [
            {"url": link["url"], "alt": link["alt"]}
            for link in links
            if self.valid_image_link(link)
        ]
        return filtered_links

    def make_link_absolute(self, url, base_url):
        if url.startswith("http://") or url.startswith("https://"):
            return url
        try:
            return urljoin(base_url, url)
        except ValueError:
            return url

    def make_links_absolute(self, links, base_url):
        return [
            {"url": self.make_link_absolute(link["url"], base_url), "alt": link["alt"]}
            for link in links
        ]

    def parse_content(self, record):
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
        filtered_links = self.extract_image_from_links(links)
        filtered_links = self.make_links_absolute(filtered_links, base_url)
        filtered_links = [
            link
            for link in filtered_links
            if link["url"].startswith("http://") or link["url"].startswith("https://")
        ]
        for link in filtered_links:
            link["hash"] = xxhash.xxh128((link["alt"] + link["url"])).hexdigest()
        all_links.extend(filtered_links)
        return all_links
