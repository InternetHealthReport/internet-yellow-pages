"""Delete duplicate statements that have no reference."""

import logging
import sys

from SPARQLWrapper import JSON, SPARQLWrapper

from iyp.wiki.wikihandy import DEFAULT_WIKI_SPARQL, Wikihandy


class Cleaner(object):

    def __init__(self, wikihandy=None, sparql=DEFAULT_WIKI_SPARQL):
        """Initialize SPARQL and wikihandy.

        wikihandy: a Wikihandy instance to use. A new will be created if
        this is set to None.
        """

        logging.info('remove_duplicate_statements initialization...\n')
        if wikihandy is None:
            self.wh = Wikihandy(preload=False)
        else:
            self.wh = wikihandy

        self.sparql = SPARQLWrapper(sparql)

    def run(self):
        """Find duplicate statements and remove them."""

        for item_qid in self.all_qid():
            self.remove_duplicate(item_qid)

    def all_qid(self):

        sys.stderr.write('fetching all items QID...\n')
        QUERY = """
        SELECT DISTINCT ?item
        WHERE {
            ?item wdt:P1 ?stmt_value.
        }
        """

        # Query wiki
        self.sparql.setQuery(QUERY)
        self.sparql.setReturnFormat(JSON)
        response = self.sparql.query().convert()
        results = response['results']

        # Parse resul
        for i, res in enumerate(results['bindings']):
            sys.stderr.write(f'\r{i}/{len(results["bindings"])} items')
            yield res['item']['value'].rpartition('/')[2]

    def remove_duplicate(self, qid):

        # Get the ItemPage object
        item = self.wh.get_item(qid=qid)

        # fetch item properties
        item.get()

        for prop, claims in item.claims.items():

            # Skip if there is only one claim
            if len(claims) < 2:
                continue

            claims_to_remove = []
            for i, claim in enumerate(claims):
                # skip claims with references, it should be removed by the
                # corresponding crawler
                if len(claim.sources) > 0:
                    continue

                for claim2 in claims[i + 1:]:
                    if len(claim2.sources) > 0:
                        continue

                    if claim2.target == claim.target:
                        print(f'delete for item {qid}: \t', claim2)
                        print('same as: \t', claim)

                        claims_to_remove.append(claim2)

                        # Avoid deleting more than 500 claims at once
                        if len(claims_to_remove) > 300:
                            item.removeClaims(claims_to_remove)
                            claims_to_remove = []

                if len(claims_to_remove) > 0:
                    # Remove claims
                    item.removeClaims(claims_to_remove)
                    break


if __name__ == '__main__':

    clean = Cleaner()
    clean.run()
