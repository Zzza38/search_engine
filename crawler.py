import requests
from urllib.parse import urljoin, urlparse, urlunparse
import re
from collections import defaultdict
from tqdm import tqdm
import time
import json
from urllib.robotparser import RobotFileParser

def welcome():
    print('Welcome to the website crawler.')
    print('What do you want to do?')
    print('1 - crawl websites')
    print('2 - exit')
    option = input('Enter your choice: ')
    return option

def rootURL(url):
    parsed = urlparse(url)
    root = urlunparse((parsed.scheme, parsed.netloc, '/', '', '', ''))
    return root

def get_site_root(url):
    """Returns the site root, stripping subdomains and paths."""
    parsed = urlparse(url)
    # Remove subdomains
    domain_parts = parsed.netloc.split('.')
    if len(domain_parts) > 2:
        domain_parts = domain_parts[-2:]  # Keep the last two parts
    site_root = parsed.scheme + "://" + ".".join(domain_parts)
    return site_root

def get_robots_txt(url):
    root_url = rootURL(url)
    robots_url = root_url + 'robots.txt'
    try:
        response = requests.get(robots_url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching robots.txt from {robots_url}: {e}")
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
            response = requests.get(url)
            
            if response.status_code == 429:
                print(f"Received 429 for {url}. Retrying in {delay} seconds...")
                time.sleep(delay)
                attempt += 1
                continue

            response.raise_for_status()
            data = response.text
            break

        except requests.RequestException as e:
            print(f"Error fetching the URL {url}: {e}")
            return []
    
    if attempt == retries:
        print(f"Max retries reached for {url}. Skipping.")
        return []

    url_pattern = re.compile(r'href=["\'](https?://[^\s"\']+|/[^\s"\']+|[\./\w\-]+\.html?)["\']')
    urls = []
    matches = url_pattern.findall(data)
    for match in matches:
        full_url = urljoin(url, match)
        urls.append(full_url)

    html_urls = [u for u in urls if re.match(r'https?://[^\s]+(?:\.html?)?$', u)]
    return html_urls

def calculate_vote_weight(current_root, target_root):
    if current_root == target_root:
        return 0.1  # Same site
    elif target_root.endswith("." + current_root) or current_root.endswith("." + target_root):
        return 0.25  # Sibling/child relationship
    else:
        return 1  # Unrelated site

def process_urls(urls, depth, visited, to_visit, max_depth, progress_bar, robots_txt=None, result_dict=None, vote_counts=None):
    for url in urls:
        if url in visited:
            continue
        visited.add(url)

        current_root = get_site_root(url)

        found_urls = crawl_site(url, robots_txt)
        if url not in result_dict:
            result_dict[url] = []

        for new_url in found_urls:
            new_root = get_site_root(new_url)
            weight = calculate_vote_weight(current_root, new_root)

            vote_counts[new_root] += weight
            if new_url not in visited and depth < max_depth:
                to_visit.append((new_url, depth + 1))
                result_dict[url].append(new_url)

        progress_bar.total = len(visited) + len(to_visit)
        progress_bar.set_postfix({'Generation': f'{depth}', "Remaining": len(to_visit)})
        progress_bar.update(1)

def search_all_urls(start_url, max_depth, robots_txt=None):
    visited = set()
    to_visit = [(start_url, 1)]
    result_dict = {}
    vote_counts = defaultdict(float)  # Use float to accommodate fractional weights

    progress_bar = tqdm(total=1000, dynamic_ncols=True, desc="Crawling", unit=" URLs")

    while to_visit:
        url, depth = to_visit.pop(0)
        process_urls([url], depth, visited, to_visit, max_depth, progress_bar, robots_txt, result_dict, vote_counts)

    progress_bar.close()
    return result_dict, vote_counts

if __name__ == "__main__":
    choice = welcome()

    if choice != '2':
        print('To crawl websites, we need a starting point.')
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

        output_crawl_path = './crawl_result.json'
        output_votes_path = './vote_counts.json'

        with open(output_crawl_path, 'w') as crawl_file:
            json.dump(crawl_result, crawl_file, indent=4)

        with open(output_votes_path, 'w') as votes_file:
            json.dump(vote_counts, votes_file, indent=4)

        print(f"Crawl results saved to '{output_crawl_path}'.")
        print(f"Vote counts saved to '{output_votes_path}'.")
