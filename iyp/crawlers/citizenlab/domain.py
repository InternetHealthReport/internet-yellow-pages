import sys
import logging
import requests
from bs4 import BeautifulSoup
from iyp import BaseCrawler

# Organization name and URL to data
ORG = 'Citizen Lab'
URL = 'https://github.com/citizenlab/test-lists/blob/master/lists/'
NAME = 'citizenlab.domain'  # should reflect the directory and name of this file


class Crawler(BaseCrawler):
    # Base Crawler provides access to IYP via self.iyp
    # and setup a dictionary with the org/url/today's date in self.reference
    # URL = 'https://github.com/citizenlab/test-lists/blob/master/lists/'
    URL = 'https://github.com/citizenlab/test-lists/blob/master/lists/'

    def run(self):
        """Fetch data and push to IYP. """

        # Fetch country code
        url_for_country_codes = "https://github.com/citizenlab/test-lists/blob/master/lists/00-LEGEND-country_codes.csv"
        req_for_country_codes = requests.get(url_for_country_codes)

        if req_for_country_codes.status_code != 200:
            logging.error('Cannot download data {req.status_code}: {req.text}')
            sys.exit('Error while fetching data file')

        soup = BeautifulSoup(req_for_country_codes.content, "html.parser")
        lists = soup.find_all("tr", class_="js-file-line")

        country_codes = []

        print("Fetching country codes...")
        for list in lists:
            country_code = list.select_one(":nth-child(2)")
            if len(country_code.text) != 2:
                continue
            country_codes.append(country_code.text.lower())

        # Fetch from each csv file using country code
        all_info = []
        success = 0
        failed = 0
        for code in country_codes[0:5]:
            url = self.generate_url(code)
            req_with_respect_to_country_code = requests.get(url)
            if req_with_respect_to_country_code.status_code != 200:
                print('Cannot fetch for country code', code, ' !')
                failed += 1
                continue
            print('Fetching domains for country code ', code, ' ...')
            success += 1
            country_soup = BeautifulSoup(req_with_respect_to_country_code.content, 'html.parser')
            rows = country_soup.find_all("tr", class_="js-file-line")
            for row in rows:
                domain_name = row.select_one(":nth-child(2)").text
                category_code = row.select_one(":nth-child(3)").text
                category_description = row.select_one(":nth-child(4)").text
                info = dict(domain=domain_name, category_code=category_code, category_description=category_description)
                all_info.append(info)
        print(all_info)
        # print("result length", len(all_info))
        # print("Success: ", success)
        # print("Failed: ", failed)
        # print("Total: ", success + failed)

        # Process line one after the other
        # for i, line in enumerate(req.text.splitlines()):
        #     self.update(line)
        #     sys.stderr.write(f'\rProcessed {i} lines')
        #
        # sys.stderr.write('\n')

    def generate_url(self, code):
        base_url = self.URL
        joined_url = "".join([base_url, code, ".csv"])
        return joined_url

    # def update(self, one_line):
    #     """Add the entry to IYP if it's not already there and update its
    #     properties."""
    #
    #     asn, value = one_line.split(',')
    #
    #     # create node for value
    #     val_qid = self.iyp.get_node(
    #         'EXAMPLE_NODE_LABEL',
    #         {
    #             'example_property_0': value,
    #             'example_property_1': value,
    #         },
    #         create=True
    #     )
    #
    #     # set relationship
    #     statements = [['EXAMPLE_RELATIONSHIP_LABEL', val_qid, self.reference]]
    #
    #     # Commit to IYP
    #     # Get the AS's node ID (create if it is not yet registered) and commit changes
    #     as_qid = self.iyp.get_node('AS', {'asn': asn}, create=True)
    #     self.iyp.add_links(as_qid, statements)


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

    domain = Crawler(ORG, URL, NAME)
    domain.run()
    domain.close()
