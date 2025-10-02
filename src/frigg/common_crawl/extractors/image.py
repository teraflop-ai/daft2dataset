from frigg.common_crawl.extractors.base import Extractor


class ImageExtractor(Extractor):
    def valid_image_link(self, link):
        valid_path = link.get("path", "") == "IMG@/src"
        valid_alt = len(link.get("alt", "")) > 0
        return valid_path and valid_alt

    def extract_from_links(self, links):
        filtered_links = [
            {"url": link["url"], "alt": link["alt"]}
            for link in links
            if self.valid_image_link(link)
        ]
        return filtered_links
