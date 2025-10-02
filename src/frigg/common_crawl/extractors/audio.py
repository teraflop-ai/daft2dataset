from frigg.common_crawl.extractors.base import Extractor


class AudioExtractor(Extractor):
    def valid_audio_link(self, link):
        valid_audio = any(
            link.get("url", "").endswith(ext)
            for ext in [".ogg", ".wav", ".mp3", ".flac", ".m4a"]
        )
        return valid_audio

    def extract_from_links(self, links):
        filtered_links = [
            {"url": link["url"], "alt": link.get("text", "")}
            for link in links
            if self.valid_audio_link(link)
        ]
        return filtered_links
