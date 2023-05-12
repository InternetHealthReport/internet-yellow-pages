from iyp.crawlers.rapid7.forward_dns_v4 import Crawler

URL = 'https://opendata.rapid7.com/sonar.fdns_v2/2021-02-26-1614297920-fdns_aaaa.json.gz'

if __name__ == '__main__':
    crawler = Crawler(fdns_url=URL)
    crawler.run()
