import sys
import logging
import requests
from bs4 import BeautifulSoup
from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Citizen Lab'
URL = 'https://github.com/citizenlab/test-lists/blob/master/lists/'
NAME = 'citizenlab.urldb'  # should reflect the directory and name of this file


def generate_url(suffix):
    base_url = URL
    joined_url = "".join([base_url, suffix, ".csv"])
    return joined_url


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and set up a dictionary with the org/url/today's date in self.reference

    def run(self):
        """Fetch data and push to IYP. """

        # Fetch country code
        url_for_country_codes = generate_url('00-LEGEND-country_codes')
        req_for_country_codes = requests.get(url_for_country_codes)

        if req_for_country_codes.status_code != 200:
            logging.error('Cannot download data {req.status_code}: {req.text}')
            sys.exit('Error while fetching data file')

        soup = BeautifulSoup(req_for_country_codes.content, "html.parser")
        rows = soup.find_all("tr", class_="js-file-line")

        country_codes = []
        for row in rows:
            country_code = row.select_one(":nth-child(2)")
            if len(country_code.text) != 2:
                continue
            country_codes.append(country_code.text.lower())

        # Fetch from each csv file using country code
        lines = []
        urls = set()
        categories = set()
        for code in country_codes:
            url = generate_url(code)
            req_with_respect_to_country_code = requests.get(url)
            if req_with_respect_to_country_code.status_code != 200:
                continue
            country_soup = BeautifulSoup(req_with_respect_to_country_code.content, 'html.parser')
            rows = country_soup.find_all("tr", class_="js-file-line")
            for row in rows:
                url = row.select_one(":nth-child(2)").text
                # category_code = row.select_one(":nth-child(3)").text
                category = row.select_one(":nth-child(4)").text
                urls.add(url)
                categories.add(category)
                if [url, category] in lines:
                    continue
                lines.append([url, category])
        url_id = self.iyp.batch_get_nodes('URL', 'url', urls)
        category_id = self.iyp.batch_get_nodes('Tag', 'label', categories)

        links = []
        for (url, category) in lines:
            url_qid = url_id[url]
            category_qid = category_id[category]
            links.append({'src_id': url_qid, 'dst_id': category_qid, 'props': [self.reference]})  # Set URL category

        # Push all links to IYP
        self.iyp.batch_add_links('CATEGORIZED', links)


# Main program
if __name__ == '__main__':
    script_name = sys.argv[0].replace('/', '_')[0:-3]
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + script_name + '.log',
        level=logging.WARNING,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Started: %s" % sys.argv)

    urldb = Crawler(ORG, URL, NAME)
    urldb.run()
    urldb.close()

    logging.info("End: %s" % sys.argv)
