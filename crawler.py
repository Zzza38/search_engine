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
visited = set()
result_dict = {}
vote_counts = defaultdict(float)

def rootURL(url):
    parsed = urlparse(url)
    root = urlunparse((parsed.scheme, parsed.netloc, '/', '', '', ''))
    return root

def get_site_root(url):
    parsed = urlparse(url)
    domain_parts = parsed.netloc.split('.')
    if len(domain_parts) > 2:
        domain_parts = domain_parts[-2:]
    site_root = parsed.scheme + "://" + ".".join(domain_parts)
    return site_root

def get_robots_txt(url):
    root_url = rootURL(url)
    robots_url = root_url + 'robots.txt'
    try:
        headers = {'User-Agent': 'Seekora'}
        response = requests.get(robots_url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching robots.txt from {robots_url}: {e}")
        return None

def is_allowed_to_crawl(robots_txt, url):
    robot_parser = RobotFileParser()
    robot_parser.parse(robots_txt.splitlines())
    return robot_parser.can_fetch('*', url)

def crawl_site(url, robots_txt=None, retries=3, delay=5):
    if robots_txt and not is_allowed_to_crawl(robots_txt, url):
        return []

    attempt = 0
    while attempt < retries:
        try:
            headers = {'User-Agent': 'Seekora'}
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                logging.warning(f"Received 429 for {url}. Retrying in {delay} seconds...")
                time.sleep(delay)
                attempt += 1
                continue

            response.raise_for_status()
            return response.text

        except requests.RequestException as e:
            logging.error(f"Error fetching the URL {url}: {e}")
            return None

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

def fetcher_thread(start_urls, robots_txt, max_depth):
    """Fetch HTML documents and store them in a queue."""
    while start_urls:
        with visited_lock:
            if not start_urls:
                break
            url, depth = start_urls.pop(0)
            if url in visited or depth > max_depth:
                continue
            visited.add(url)

        html = crawl_site(url, robots_txt)
        if html:
            html_queue.put((url, html, depth))

def processor_thread(start_urls, max_depth):
    """Process HTML documents from the queue and extract links."""
    while True:
        try:
            url, html, depth = html_queue.get(timeout=1)
        except:
            if not start_urls:
                break
            continue

        found_urls = extract_links(html, url)
        current_root = get_site_root(url)

        with result_lock:
            if url not in result_dict:
                result_dict[url] = []

            for new_url in found_urls:
                new_root = get_site_root(new_url)
                weight = calculate_vote_weight(current_root, new_root)
                vote_counts[new_root] += weight

                with visited_lock:
                    if new_url not in visited and depth + 1 <= max_depth:
                        start_urls.append((new_url, depth + 1))
                        result_dict[url].append(new_url)

        html_queue.task_done()

def tqdm_thread(total, start_urls):
    """Run TQDM progress bar."""
    with tqdm(total=total, dynamic_ncols=True, desc="Crawling", unit=" URLs") as pbar:
        while True:
            with visited_lock:
                processed = len(visited)
                to_visit = len(start_urls)

            pbar.total = processed + to_visit
            pbar.n = processed
            pbar.refresh()

            time.sleep(0.1)  # Reduce CPU usage
            if not start_urls and html_queue.empty():
                break

def calculate_vote_weight(current_root, target_root):
    if current_root == target_root:
        return 0.1
    elif target_root.endswith("." + current_root) or current_root.endswith("." + target_root):
        return 0.25
    else:
        return 1

def search_all_urls(start_url, max_depth, robots_txt=None):
    """Search all URLs using multithreaded fetchers and a single processor."""
    global visited, result_dict, vote_counts
    visited = set()
    result_dict = {}
    vote_counts = defaultdict(float)

    start_urls = [(start_url, 1)]
    total_threads = os.cpu_count() - 1  # Reserve 1 thread for system

    fetch_threads = total_threads - 2
    if fetch_threads < 1:
        fetch_threads = 1
    # process_threads = 1 ### Unused

    fetchers = [Thread(target=fetcher_thread, args=(start_urls, robots_txt, max_depth)) for _ in range(fetch_threads)]
    processor = Thread(target=processor_thread, args=(start_urls, max_depth))
    progress = Thread(target=tqdm_thread, args=(1000, start_urls))

    for thread in fetchers + [processor, progress]:
        thread.start()

    for thread in fetchers + [processor, progress]:
        thread.join()

    return result_dict, vote_counts

if __name__ == "__main__":
    if os.cpu_count() < 4:
        print('You do not have enough threads to run this program.')
        os.abort()
    print('Welcome to the website crawler.')
    website = input('What website would you like to start with: ')

    if not website.startswith('https://'):
        website = 'https://' + website

    depth = int(input('How deep should the crawl go (generations)? '))

    robots_txt = get_robots_txt(website)

    start_time = time.time()
    crawl_result, vote_counts = search_all_urls(website, depth, robots_txt)
    end_time = time.time()

    total_time = end_time - start_time
    print(f"\nCrawling completed in {total_time:.2f} seconds.")

    output_folder = './crawl_results/'
    output_crawl_path = 'crawl_paths.json'
    output_votes_path = 'vote_counts.json'

    with open(output_folder + output_crawl_path, 'w') as crawl_file:
        json.dump(crawl_result, crawl_file, indent=4)

    with open(output_folder + output_votes_path, 'w') as votes_file:
        json.dump(vote_counts, votes_file, indent=4)

    print(f"Crawl results saved to '{output_crawl_path}'.")
    print(f"Vote counts saved to '{output_votes_path}'.")
