import json
import queue
import requests
from bs4 import BeautifulSoup
from time import sleep

"""
Curlie Scraper 
Records sites under each category and scrapes subcategories, 
Then performs the same process on all subcategories
"""

VISITED = set()
QUEUE = queue.Queue()


def search(sesh: requests.Session, category: str):
    """submits a query to Curlie using requests
    Args:
        sesh (requests.Session): requests session
        category (str): category
    Returns:
        BeautifulSoup: html-parsed response content
    """
    r = sesh.get(
        "https://www.curlie.org/" + category,
        timeout=30,
    ).text
    return BeautifulSoup(r, "lxml")


def get_sites(html: BeautifulSoup, category: str) -> list:
    """
    Scrapes sites from Curlie page
    :param html: BeautifulSoup-parsed html
    :param category: current category
    :return: dictionary mapping business name to url and category
    """
    sites = []
    start = html.find_all("div", class_="site-title")
    for each in start:
        site = each.find("a", target="_blank")
        if site not in sites:
            current = {
                "Name": site.text,
                "URL": site.get("href"),
                "Category": category,
            }
            sites.append(current)
    return sites


def get_subcategories(html: BeautifulSoup) -> list:
    """
    Gets subcategories for each site and calls recursively
    :param html: BeautifulSoup parsed html
    :param sesh: requests session
    :return: all scraped sites
    """
    start = html.find_all("div", class_="cat-list results leaf-nodes")
    all_cats = []
    for each in start:
        cat_cmpts = each.find_all("div", class_="cat-item")
        cats = [cmpt.find("a").get("href") for cmpt in cat_cmpts]
        all_cats.extend(cats)
    return all_cats


def write_to_file(sites: list, filename: str) -> None:
    """
    Writes sites to file by name, url, and category
    :param sites: list of scraped sites
    :param filename: file being written to
    :return: nothing
    """
    with open(filename, mode="a") as outfile:
        for site in sites:
            outfile.write(json.dumps(site))
            outfile.write("\n")


def scrape_category(
    sesh: requests.Session, count: int, filename: str, parent_cat: str
) -> int:
    """
    1) requests category, 2) gets subcategories, 3) gets sites, 4) writes sites to file
    :param sesh: requests session
    :param count: current requests count
    :param filename: file destination for results
    :return: nothing
    """
    category = QUEUE.get()

    # check if category already visited
    if category in VISITED:
        return count

    print(f"Scraping {category}. Queue size: {QUEUE.qsize()}")
    if count == 50:
        sesh.close()
        sesh = requests.Session()
        count = 0

    count += 1
    page_text = search(sesh, category).find("div")
    sleeptime = 600

    while page_text is None:
        sleep(sleeptime)
        page_text = search(sesh, category).find("div")
        sleeptime += 600

    sites = get_sites(page_text, category)

    write_to_file(sites, filename)
    VISITED.add(category)

    # get subcategories, put in queue
    sub_cats = get_subcategories(page_text)
    for sub_cat in sub_cats:
        if check_cat(sub_cat, parent_cat):
            print(f"Just added {sub_cat}")
            QUEUE.put(sub_cat)
    return count

def check_cat(category: str, parent_cat: str) -> bool:
    """
    Helper function for scrape_category to make sure category is in parent directory
    before being scraped
    Args:
        category: subcategory that's been scraped
        parent_cat: parent directory category
    Returns: boolean

    """
    cat = category.split("/")
    if cat[2] == parent_cat:
        return True
    else:
        return False


def main():
    sesh = requests.Session()
    count = 0

    parent_cat = input("Enter a category to scrape: ")
    QUEUE.put("/en/" + parent_cat)
    print(f"Just added {parent_cat}")
    filename = input("Name of file to store scraped results: ")

    while QUEUE.qsize() > 0:
        scrape_category(sesh, count, filename, parent_cat)

    parent_cat = input("Enter another category to scrape, q to quit: ")
    while parent_cat != "q":
        sesh.close()
        count = 0

        sesh = requests.session()
        QUEUE.put("/en/" + parent_cat)
        print(f"Just added {parent_cat}")

        while QUEUE.qsize() > 0:
            scrape_category(sesh, count, filename, parent_cat)

        parent_cat = input("Enter another category to scrape, q to quit: ")

    sesh.close()


if __name__ == "__main__":
    main()
