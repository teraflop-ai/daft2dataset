from frigg.common_crawl.extractors.base import Extractor


class VideoExtractor(Extractor):
    def valid_video_link(self, link):
        valid_video = any(
            link.get("url", "").endswith(ext)
            for ext in [
                ".avi",
                ".mp4",
                ".mkv",
                ".webm",
                ".mov",
                ".mpg",
                ".mpeg",
                ".m4v",
            ]
        )
        return valid_video

    def extract_from_links(self, links):
        filtered_links = [
            {"url": link["url"], "alt": link.get("text", "")}
            for link in links
            if self.valid_video_link(link)
        ]
        return filtered_links
