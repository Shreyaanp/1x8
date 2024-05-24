import scrapy
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import os

class CssSpider(scrapy.Spider):
    name = "cs_spider"
    allowed_domains = ['geeksforgeeks.org']
    start_urls = [
        'https://basescripts.com/110-javascript-quiz-questions-with-solutions-pdf-download-test-your-knowledge'
    ]
    processed_urls = set()

    def __init__(self, *args, **kwargs):
        super(CssSpider, self).__init__(*args, **kwargs)
        self.load_processed_urls()

    def load_processed_urls(self):
        if os.path.exists('raw_data.json'):
            try:
                with open('raw_data.json', 'r') as file:
                    data = json.load(file)
                    for item in data:
                        self.processed_urls.add(item['url'])
            except json.JSONDecodeError as e:
                self.log(f"Error loading JSON file: {e}")

    def parse(self, response):
        self.log(f'Processing {response.url}')
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract all href links
        links = soup.find_all('a', href=True)
        for link in links:
            url = link['href']
            # Filter out URLs with hash fragments and external links
            if self.is_valid_url(url) and url not in self.processed_urls:
                self.processed_urls.add(url)
                yield scrapy.Request(url, callback=self.parse_page_content)
    
    def parse_page_content(self, response):
        self.log(f'Processing {response.url}')
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract paragraphs and other relevant content
        paragraphs = soup.find_all('p')
        text = '\n'.join([p.get_text() for p in paragraphs])
        
        yield {'url': response.url, 'text': text}

    def is_valid_url(self, url):
        # Check if the URL belongs to the allowed domain and does not contain hash fragments
        parsed_url = urlparse(url)
        if parsed_url.netloc == 'www.geeksforgeeks.org' and not parsed_url.fragment:
            return True
        return False

# To run the spider, save this script as `cs_spider.py` inside a Scrapy project and use the following command:
# scrapy crawl cs_spider -o raw_data.json
