import requests
from urllib.parse import urljoin, urlparse, urlunparse
import re
from collections import defaultdict
from tqdm import tqdm
import time
import json
from urllib.robotparser import RobotFileParser
from threading import Thread, Lock
from queue import Queue
import os
import logging

# Configure logging
logging.basicConfig(
    filename='crawler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Thread-safe queues
html_queue = Queue()
result_lock = Lock()
visited_lock = Lock()

# Shared results and state
robots_txt = {}
visited = set()
result_dict = {}
vote_counts = defaultdict(float)

def rootURL(url):
    parsed = urlparse(url)
    root = urlunparse((parsed.scheme, parsed.netloc, '/', '', '', ''))
    return root

def get_robots_txt(url):
    root_url = rootURL(url)
    robots_url = root_url + 'robots.txt'
    try:
        headers = {'User-Agent': 'Seekora'}
        response = requests.get(robots_url, headers=headers)
        response.raise_for_status()
        robots_txt[root_url] = response.text
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching robots.txt from {robots_url}: {e}")
        return None

def is_allowed_to_crawl(robots_txt_content, url):
    robot_parser = RobotFileParser()
    robot_parser.parse(robots_txt_content.splitlines())
    return robot_parser.can_fetch('*', url)

def crawl_site(url, retries=3, delay=5):
    root_url = rootURL(url)
    if root_url in robots_txt:
        robots = robots_txt[root_url]
    else:
        robots = get_robots_txt(url)
    if robots and not is_allowed_to_crawl(robots, url):
        return None

    for attempt in range(retries):
        try:
            headers = {'User-Agent': 'Seekora'}
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                logging.warning(f"Received 429 for {url}. Retrying in {delay} seconds...")
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(f"Error fetching the URL {url}: {e}")
            time.sleep(delay)
    return None

def extract_links(html, base_url):
    """Extract links from HTML content."""
    url_pattern = re.compile(r'href=["\'](https?://[^\s"\']+|/[^\s"\']+|[\./\w\-]+\.html?)["\']')
    urls = []
    matches = url_pattern.findall(html)
    for match in matches:
        full_url = urljoin(base_url, match)
        urls.append(full_url)

    html_urls = [u for u in urls if re.match(r'https?://[^\s]+(?:\.html?)?$', u)]
    return html_urls

def fetcher_thread(max_depth, progress_bar):
    """Fetch HTML documents and store them in a queue."""
    while True:
        with visited_lock:
            if not start_urls:
                break
            url, depth = start_urls.pop(0)
            if url in visited or depth > max_depth:
                continue
            visited.add(url)
            progress_bar.update(1)

        html = crawl_site(url)
        if html:
            html_queue.put((url, html, depth))

def processor_thread(max_depth, progress_bar):
    """Process HTML documents from the queue and extract links."""
    while True:
        try:
            url, html, depth = html_queue.get(timeout=1)
        except:
            if not start_urls:
                break
            continue

        found_urls = extract_links(html, url)
        with result_lock:
            if url not in result_dict:
                result_dict[url] = []

            for new_url in found_urls:
                with visited_lock:
                    if new_url not in visited and depth + 1 <= max_depth:
                        start_urls.append((new_url, depth + 1))
                        result_dict[url].append(new_url)

        html_queue.task_done()

def search_all_urls(start_url, max_depth):
    """Search all URLs using multithreaded fetchers and a single processor."""
    global result_dict, vote_counts, start_urls
    result_dict = {}
    vote_counts = defaultdict(float)

    start_urls = [(start_url, 1)]
    total_urls = len(start_urls)

    with tqdm(total=total_urls, dynamic_ncols=True, desc="Crawling", unit="URLs") as progress_bar:
        fetch_threads = []
        for _ in range(max(1, os.cpu_count() - 2)):
            thread = Thread(target=fetcher_thread, args=(max_depth, progress_bar))
            thread.start()
            fetch_threads.append(thread)

        processor = Thread(target=processor_thread, args=(max_depth, progress_bar))
        processor.start()

        for thread in fetch_threads:
            thread.join()
        processor.join()

    return result_dict, vote_counts

if __name__ == "__main__":
    print('Welcome to the website crawler.')
    website = input('What website would you like to start with: ')

    if not website.startswith('https://'):
        website = 'https://' + website

    depth = int(input('How deep should the crawl go (generations)? '))
    start_time = time.time()

    crawl_result, vote_counts = search_all_urls(website, depth)

    end_time = time.time()
    print(f"\nCrawling completed in {end_time - start_time:.2f} seconds.")

    output_folder = './crawl_results/'
    output_crawl_path = 'crawl_paths.json'
    output_votes_path = 'vote_counts.json'

    os.makedirs(output_folder, exist_ok=True)

    with open(output_folder + output_crawl_path, 'w') as crawl_file:
        json.dump(crawl_result, crawl_file, indent=4)

    with open(output_folder + output_votes_path, 'w') as votes_file:
        json.dump(vote_counts, votes_file, indent=4)

    print(f"Crawl results saved to '{output_folder + output_crawl_path}'.")
    print(f"Vote counts saved to '{output_folder + output_votes_path}'.")
