import argparse
import logging
import sys

from iyp import BasePostProcess

NAME = 'post.address_family'


class PostProcess(BasePostProcess):
    def run(self):
        """Add address family (4 or 6 for IPv4 or IPv6) to all IP and Prefix nodes."""

        # Update prefixes
        self.iyp.tx.run("MATCH (pfx:Prefix) WHERE pfx.prefix CONTAINS '.' SET pfx.af = 4")
        self.iyp.commit()
        self.iyp.tx.run("MATCH (pfx:Prefix) WHERE pfx.prefix CONTAINS ':' SET pfx.af = 6")
        self.iyp.commit()

        # Update IP addresses
        self.iyp.tx.run("MATCH (ip:IP) WHERE ip.ip CONTAINS '.' SET ip.af = 4")
        self.iyp.commit()
        self.iyp.tx.run("MATCH (ip:IP) WHERE ip.ip CONTAINS ':' SET ip.af = 6")
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
