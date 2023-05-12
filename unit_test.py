import json
import subprocess


def run_crawler(crawler):
    print('Running Crawler: ' + crawler)
    subprocess.call(['python3', '-m', crawler, 'unit_test'])


def run_post_script(post_script):
    print('Running Post Script: ' + post_script)
    subprocess.call(['python3', '-m', post_script, 'unit_test'])


with open('config.json') as config_str:

    config = json.load(config_str)

    # read crawlers info and starrt unit testing of the crawlers
    crawlers = config['iyp']['crawlers']
    for crawler in crawlers:
        run_crawler(crawler)

    # read post scripts info and start unit testing of the post scripts
    post_scripts = config['post']
    for post_script in post_scripts:
        run_post_script(post_script)
