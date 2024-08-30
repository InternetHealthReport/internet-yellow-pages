import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import docker
import requests

NEO4J_VERSION = '5.16.0'

ARCHIVE_URL_SUFFIX = '%Y/%m/%d/iyp-%Y-%m-%d'
LOG_URL_SUFFIX = ARCHIVE_URL_SUFFIX + '.log'
DUMP_URL_SUFFIX = ARCHIVE_URL_SUFFIX + '.dump'

DUMP_DOWNLOAD_DIR_SUFFIX = 'dumps/%Y/%m/%d'

DOCKER_VOLUME_FMT = 'data-%m-%d'
DOCKER_CONTAINER_NAME_FMT = 'deploy-%m-%d'


def remove_deployment(client: docker.DockerClient, date: datetime):
    """Checks if there is an active deployment for the given date (month-day).

    If there is, remove it and the corresponding volume storing its data.
    """
    container_name = date.strftime(DOCKER_CONTAINER_NAME_FMT)
    volume_name = date.strftime(DOCKER_VOLUME_FMT)
    try:
        container = client.containers.get(container_name)
        logging.warning(f'Removing active deployment for {date.strftime("%m-%d")}')
        container.stop()
        # Wait a little bit after the container has been removed
        # before deleting the volume
        # TODO Is there a better way?
        while True:
            try:
                client.volumes.get(volume_name).remove()
                break
            except BaseException:
                time.sleep(1)
    except docker.errors.NotFound:
        logging.info(f'No existing deployment for {date.strftime("%Y-%m-%d")}. Starting deployment')


def get_ports_from_caddy_config(config: dict):
    """Makes a request to caddy config and returns the ports currently being used (both
    active and previous)"""
    caddy_config_url = config['caddy_config_url']
    r = requests.get(caddy_config_url)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        logging.error(f'Failed to retrieve Caddy config from {caddy_config_url}: {e}')
        sys.exit(1)
    try:
        body = r.json()
    except json.JSONDecodeError as e:
        logging.error(f'Failed to parse Caddy config: {e}')
        sys.exit(1)

    routes = body['apps']['http']['servers']['srv0']['routes']
    active = dict()
    for route in routes:
        # This happens with a fresh caddy build. No ports are active, so
        # return an empty dict.
        # TODO So there is a route dict, but without a 'match' entry?
        if 'match' not in route:
            return dict()
        host = route['match'][0]['host'][0]
        dial = route['handle'][0]['routes'][0]['handle'][0]['upstreams'][0]['dial']
        active[host] = dial

    # TODO We want to have both active_bolt and active_http URLs in the config, right?
    # If only one is present, we have a problem.
    # Also, if we reach this point there _must_ be at least a currently active
    # deployment?
    ports = dict()
    active_bolt = config['urls']['active_bolt']
    active_http = config['urls']['active_http']
    if active_bolt in active:
        ports['active_bolt'] = active[active_bolt].split(':')[1]
    if active_http in active:
        ports['active_http'] = active[active_http].split(':')[1]

    # It's possible for there to be only one active deployment, and there
    # are no previous ports. Only attempt to parse the previous ports
    # if they have been filled in on the caddy config
    prev_bolt = config['urls']['prev_bolt']
    prev_http = config['urls']['prev_http']
    if prev_bolt in active and 'PREV_BOLT_PORT' not in active[prev_bolt]:
        ports['prev_bolt'] = active[prev_bolt].split(':')[1]
    if prev_http in active and 'PREV_HTTP_PORT' not in active[prev_http]:
        ports['prev_http'] = active[prev_http].split(':')[1]
    return ports


def check_log(config: dict, date: datetime):
    """Makes a request to archive and checks if there is a valid dump for the specified
    date."""
    logging.info(f'Downloading logs for {date.strftime("%Y-%m-%d")}')
    log_url_fmt = os.path.join(config['archive_base_url'], LOG_URL_SUFFIX)
    log_url = date.strftime(log_url_fmt)
    r = requests.get(log_url)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # We expect the request to fail if the log does not exist (404), but not for
        # other reasons.
        if r.status_code != 404:
            logging.error(f'Expected HTTP code 200 or 404, but got: {e}')
            sys.exit(1)
        return False
    body = r.content
    last_line = body.decode().split('\n')[-1]
    if 'Errors:' in last_line:
        logging.error(f'There were errors from create_db found in logs for {log_url}')
        sys.exit(1)
    return True


def get_port_date(port):
    """Extracts the month and day from a port.

    Port should have format [1|2]MMDD.

    Returns the tuple (month, day)
    """
    month = int(port[-4:-2])
    day = int(port[-2:])
    return month, day


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-d', '--date', help='deploy IYP dump for this date (YYYY-MM-DD)')
    args = parser.parse_args()

    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')

    with open(args.config, 'r') as f:
        try:
            config: dict = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f'Invalid configuration specified: {e}')
            sys.exit(1)

    root = os.path.dirname(os.path.realpath(__file__))

    # If no date is provided when running the script, check if any dumps
    # have been made within a week since the previous deployment. Otherwise,
    # use the date provided in command line arg.
    if args.date:
        try:
            date = datetime.strptime(args.date, '%Y-%m-%d')
        except ValueError as e:
            logging.error(f'Invalid date specified: {e}')
            sys.exit(1)
    else:
        ports = get_ports_from_caddy_config(config)
        success = False
        if 'active_http' in ports:
            active_http = ports['active_http']
            month, day = get_port_date(active_http)
            start_date = datetime.now(tz=timezone.utc).replace(month=month, day=day)
        else:
            start_date = datetime.now(tz=timezone.utc)

        # Download logs from ihr archive each day in the next week since
        # the previous release
        for i in range(1, 8):
            date = start_date + timedelta(days=i)
            if check_log(config, date):
                success = True
                break
            else:
                logging.warning(f'No archive entry found for {date.strftime("%Y-%m-%d")}.')

        if not success:
            logging.error('Exiting because no active dates were found in archive.')
            sys.exit(1)

    # Define ports and filenames that depend on the date
    volume_name = date.strftime(DOCKER_VOLUME_FMT)
    container_name = date.strftime(DOCKER_CONTAINER_NAME_FMT)
    http_port = date.strftime('1%m%d')
    bolt_port = date.strftime('2%m%d')

    client = docker.from_env()

    # Check if there is an existing deployment for this day and remove if so
    remove_deployment(client, date)

    # Download dump from ihr archive
    logging.info(f'Downloading dump for {date.strftime("%Y-%m-%d")}')
    dump_dir = os.path.join(root, date.strftime(DUMP_DOWNLOAD_DIR_SUFFIX))
    os.makedirs(dump_dir, exist_ok=True)
    dump_url_fmt = os.path.join(config['archive_base_url'], DUMP_URL_SUFFIX)
    dump_url = date.strftime(dump_url_fmt)
    r = requests.get(dump_url)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        logging.error(f'Failed to fetch dump from {dump_url}: {e}')
        sys.exit(1)
    # TODO Is the + necessary?
    with open(os.path.join(dump_dir, 'neo4j.dump'), 'wb+') as f:
        f.write(r.content)

    # Load dump into volume
    logging.info('Load dump into neo4j db')
    client.containers.run(
        'neo4j/neo4j-admin:' + NEO4J_VERSION,
        command='neo4j-admin database load neo4j --from-path=/dumps --verbose',
        name='load',
        tty=True,
        stdin_open=True,
        remove=True,
        volumes={
                volume_name: {'bind': '/data', 'mode': 'rw'},
                dump_dir: {'bind': '/dumps', 'mode': 'rw'},
        }
    )

    # Run neo4j based on data in volume just created
    logging.warning('Starting deployment container')
    client.containers.run(
        'neo4j:' + NEO4J_VERSION,
        name=container_name,
        ports={
            7474: int(http_port),
            7687: int(bolt_port)
        },
        volumes={
            volume_name: {'bind': '/data', 'mode': 'rw'},
        },
        environment={
            'NEO4J_AUTH': 'neo4j/password',
            'NEO4J_server_memory_heap_initial__size': '16G',
            'NEO4J_server_memory_heap_max__size': '16G',
        },
        detach=True,
        remove=True
    )

    # Get currently active config
    ports = get_ports_from_caddy_config(config)

    # Only delete current prev if it exists
    if 'prev_http' in ports:
        prev_month, prev_day = get_port_date(ports['prev_http'])
        # It's possible that you're trying to redeploy the current prev
        # If this condition isn't here, then the new deployment will be deleted
        # since it has the same date as prev
        if prev_month != date.month or prev_day != date.day:
            remove_deployment(client, date.replace(month=prev_month, day=prev_day))

    with open(config['caddy_template'], 'r') as f:
        caddy_template = f.read()

    caddy_template = caddy_template.replace('<BOLT_PORT>', bolt_port)
    caddy_template = caddy_template.replace('<HTTP_PORT>', http_port)

    # If there are no active ports (for example, on the first run after a fresh
    # caddy build), don't try to set prev ports
    if 'active_http' in ports:
        caddy_template = caddy_template.replace('<PREV_BOLT_PORT>', ports['active_bolt'])
        caddy_template = caddy_template.replace('<PREV_HTTP_PORT>', ports['active_http'])

    # Update config
    requests.post(config['caddy_post_url'], caddy_template, headers={'Content-Type': 'application/json'})


if __name__ == '__main__':
    main()
    sys.exit(0)
