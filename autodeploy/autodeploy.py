import datetime
import json
import logging
import os
import sys
import time

import arrow
import docker
import requests

NEO4J_VERSION = '5.16.0'
today = arrow.utcnow()
root = os.path.dirname(os.path.realpath(__file__))


FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(
    format=FORMAT,
    level=logging.WARNING,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.warning('Starting deployment')

client = docker.from_env()


def remove_deployment(month, day):
    """Checks if there is an active deployment for the given month and day.

    If there is, remove it and the corresponding volume storing its data.
    """
    try:
        container = client.containers.get(f'deploy-{month}-{day}')
        logging.warning(f'Removing active deployment for {month}-{day}')
        container.stop()
        # Wait a little bit after the container has been removed
        # before deleting the volume
        while True:
            try:
                client.volumes.get(f'data-{month}-{day}').remove()
                break
            except BaseException:
                time.sleep(0.1)
    except docker.errors.NotFound:
        logging.warning(f'No existing deployment for {date}. Starting deployment')


def get_config_ports():
    """Makes a request to caddy config and returns the ports current being used (both
    active and previous)"""
    with requests.get('http://sandbox.ihr.live:2019/config/') as response:
        body = json.loads(response.content.decode())
        routes = body['apps']['http']['servers']['srv0']['routes']
        active = {}
        for route in routes:
            # This happens with a fresh caddy build. No ports are active, so
            # return an empty dict
            if 'match' not in route:
                return {}
            host = route['match'][0]['host'][0]
            dial = route['handle'][0]['routes'][0]['handle'][0]['upstreams'][0]['dial']
            active[host] = dial

        ports = {}
        if 'ryan-bolt.ihr.live' in active:
            ports['active_bolt'] = active['ryan-bolt.ihr.live'].split(':')[1]
        if 'ryan.ihr.live' in active:
            ports['active_http'] = active['ryan.ihr.live'].split(':')[1]
        if 'ryan-prev-bolt.ihr.live' in active and 'PREV_BOLT_PORT' not in active['ryan-prev-bolt.ihr.live']:
            ports['prev_bolt'] = active['ryan-prev-bolt.ihr.live'].split(':')[1]
        if 'ryan-prev.ihr.live' in active and 'PREV_HTTP_PORT' not in active['ryan-prev.ihr.live']:
            ports['prev_http'] = active['ryan-prev.ihr.live'].split(':')[1]
        return ports


def check_log(year, month, day):
    """Makes a request to archive and checks if there is a valid dump for the specified
    date."""
    date = f'{year}-{month}-{day}'
    logging.warning(f'Downloading logs for {date}')
    with requests.get(f'https://ihr-archive.iijlab.net/ihr-dev/iyp/{year}/{month}/{day}/iyp-{date}.log') as response:
        if response.status_code == 200:
            body = response.content
            last_line = body.decode().split('\n')[-1]
            if 'Errors:' in last_line:
                logging.warning(f'There were errors from create_db found in logs for iyp-{date}.log')
                sys.exit(1)
            return True
        return False


def get_port_date(port):
    """Extracts the month and day from a port.

    Returns the tuple (month, day)
    """
    month = port[-4:-2]
    day = port[-2:]
    return month, day


# If no date is provided when running the script, check if any dumps
# have been made within a week since the previous deployment. Otherwise,
# use the date provided in command line arg.
if len(sys.argv) < 2:
    ports = get_config_ports()
    success = False
    if 'active_http' in ports:
        active_http = ports['active_http']
        month, day = get_port_date(active_http)
        start_date = datetime.date(int(today.year), int(month), int(day))
    else:
        start_date = datetime.date(int(today.year), int(today.month), int(today.day))

    # Download logs from ihr archive each day in the next week since
    # the previous release
    for i in range(1, 8):
        date = start_date + datetime.timedelta(days=i)
        date = date.strftime('%Y-%m-%d')
        logging.warning(f'Checking archive for {date}')
        year, month, day = date.split('-')
        if check_log(year, month, day):
            success = True
            break
        else:
            logging.warning(f'No archive entry found for {date}')

    if not success:
        logging.warning('Exiting because no active dates were found in archive')
        sys.exit(1)
else:
    date = sys.argv[1]
    year, month, day = sys.argv[1].split('-')


# Define ports and filenames that depend on the date
neo4j_volume = f'data-{month}-{day}'
deploy_name = f'deploy-{month}-{day}'
gui_port = f'1{month}{day}'
bolt_port = f'2{month}{day}'


# Check if there is an existing deployment for this day and remove if so
remove_deployment(month, day)


# Download dump from ihr archive
logging.warning(f'Downloading dump for {date}')
dump_dir = os.path.join(root, 'dumps', f'{year}/{month}/{day}')
os.makedirs(dump_dir, exist_ok=True)
with requests.get(f'https://ihr-archive.iijlab.net/ihr-dev/iyp/{year}/{month}/{day}/iyp-{date}.dump') as response:
    with open(f'{dump_dir}/neo4j.dump', 'wb+') as f:
        f.write(response.content)


# Load dump into volume
logging.warning('Load dump into neo4j db')
container = client.containers.run(
    'neo4j/neo4j-admin:' + NEO4J_VERSION,
    command='neo4j-admin database load neo4j --from-path=/dumps --verbose',
    name='load',
    tty=True,
    stdin_open=True,
    remove=True,
    volumes={
            neo4j_volume: {'bind': '/data', 'mode': 'rw'},
            dump_dir: {'bind': '/dumps', 'mode': 'rw'},
    }
)


# Run neo4j based on data in volume just created
logging.warning('Starting deployment container')
container = client.containers.run(
    'neo4j:' + NEO4J_VERSION,
    name=deploy_name,
    ports={
        7474: int(gui_port),
        7687: int(bolt_port)
    },
    volumes={
        neo4j_volume: {'bind': '/data', 'mode': 'rw'},
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
ports = get_config_ports()

# Only delete current prev if it exists
if 'prev_http' in ports:
    prev_month, prev_day = get_port_date(ports['prev_http'])
    # It's possible that you're trying to redeploy the current prev
    # If this condition isn't here, then the new deployment will be deleted
    # since it has the same date as prev
    if prev_month != month or prev_day != day:
        remove_deployment(prev_month, prev_day)

with open('caddy.template.json', 'r') as f:
    caddy_template = f.read()

caddy_template = caddy_template.replace('<BOLT_PORT>', bolt_port)
caddy_template = caddy_template.replace('<HTTP_PORT>', gui_port)

# If there are no active ports (for example, on the first run after a fresh
# caddy build), don't try to set prev ports
if 'active_http' in ports:
    caddy_template = caddy_template.replace('<PREV_BOLT_PORT>', ports['active_bolt'])
    caddy_template = caddy_template.replace('<PREV_HTTP_PORT>', ports['active_http'])


# Update config
requests.post('http://localhost:2019/load', caddy_template, headers={'Content-Type': 'application/json'})
