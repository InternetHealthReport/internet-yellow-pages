import argparse
import logging
import sys

import iso3166

from iyp import BasePostProcess

NAME = 'post.country_information'


class PostProcess(BasePostProcess):
    def run(self):
        """Enrich Country nodes with additional information like alpha-3 codes and
        country names."""

        country_id = self.iyp.batch_get_nodes_by_single_prop('Country', 'country_code')

        for country_code in country_id:
            if country_code not in iso3166.countries_by_alpha2:
                logging.error(f'Country code "{country_code}" is not ISO 3166-1 alpha-2 conform.')
                continue
            country_info = iso3166.countries_by_alpha2[country_code]
            new_props = {'name': country_info.apolitical_name,
                         'alpha3': country_info.alpha3}
            self.iyp.tx.run("""
                            MATCH (n:Country)
                            WHERE elementId(n) = $id
                            SET n += $props
                            """,
                            id=country_id[country_code],
                            props=new_props)
        self.iyp.commit()

    def unit_test(self):
        raise NotImplementedError()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--unit-test', action='store_true')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename='log/' + NAME + '.log',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    post = PostProcess(NAME)
    if args.unit_test:
        post.unit_test()
    else:
        post.run()
        post.close()
    logging.info(f'Finished: {sys.argv}')


if __name__ == '__main__':
    main()
    sys.exit(0)
