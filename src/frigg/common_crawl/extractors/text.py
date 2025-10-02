from frigg.common_crawl.extractors.base import Extractor


class TextExtractor(Extractor):
    def valid_text_link(self, link):
        splits = link.get("url", "").split(".")
        if len(splits) < 2:
            return False
        if splits[-1] not in [
            "pdf",
            "epub",
            "djvu",
            "mobi",
            "doc",
            "docx",
            "rtf",
            "txt",
            "odt",
            "ppt",
            "pptx",
            "pages",
            "keynote",
            "wps",
            "md",
        ]:
            return False
        return True

    def extract_from_links(self, links):
        filtered_links = [
            {"url": link["url"], "alt": link.get("text", "")}
            for link in links
            if self.valid_text_link(link)
        ]
        return filtered_links
