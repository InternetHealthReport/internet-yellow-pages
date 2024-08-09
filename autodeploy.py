# 1. Run create_db
# 2. Push the dump to ihr-archive
# 3. Run autodeploy
#   - Takes in date as argument
#   - Downloads dump with corresponding date
#   - Creates volume (name is based on the dump date)
#   - Load data from dump into the volume and start neo4j
#       - start neo4j on ports based on the date
#       - '1' + [month] + [day] for gui
#       - '2' + [month] + [day] for bolt

import logging
import sys
import os
import docker
import logging
import requests
import time
import json

NEO4J_VERSION = '5.16.0'


FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(
    format=FORMAT,
    level=logging.WARNING,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.warning('Starting deployment')

client = docker.from_env()

date = sys.argv[1]
year, month, day = date.split('-')
root = os.path.dirname(os.path.realpath(__file__))

neo4j_volume = f'data-{month}-{day}'
deploy_name = f'deploy-{month}-{day}'
gui_port = f'1{month}{day}'
bolt_port = f'2{month}{day}'


def remove_deployment(month, day):
    container = client.containers.get(f'deploy-{month}-{day}')
    logging.warning(f'Removing active deployment for {month}-{day}')
    container.stop()
    # Wait a little bit after the container has been removed before deleting the volume
    while True:
        try:
            client.volumes.get(f'data-{month}-{day}').remove()
            break
        except:
            time.sleep(0.1)

try: 
    remove_deployment(month, day)
except docker.errors.NotFound as exc:
    logging.warning(f'No existing deployment for {date}. Starting deployment')

# Alternatively, specify your own path.
# root = ''
if not root:
    sys.exit('Please configure a root path.')


# Download logs from ihr archive
logging.warning(f'Downloading logs for {date}')
with requests.get(f'https://ihr-archive.iijlab.net/ihr-dev/iyp/{year}/{month}/{day}/iyp-{date}.log') as response:
    pass


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

# # Get currently active config
response = requests.get("http://sandbox.ihr.live:2019/config/")
body = json.loads(response.content)
routes = body['apps']['http']['servers']['srv0']['routes']
active = {}
current_active_bolt_port = ''
current_active_http_port = ''
try:
    for route in routes:
        host = route['match'][0]['host'][0]
        dial = route['handle'][0]['routes'][0]['handle'][0]['upstreams'][0]['dial']
        active[host] = dial

    current_active_bolt_port = active['ryan-bolt.ihr.live'].split(':')[1]
    current_active_http_port = active['ryan.ihr.live'].split(':')[1]

    prev_http_port = active['ryan-prev.ihr.live'].split(':')[1]
    prev_day = prev_http_port[-2:]
    prev_month = prev_http_port[-4:-2]

    # It's possible to that you're trying to redeploy the current prev
    # If this condition isn't here, then the new deployment will be deleted
    # since it has the same date as prev
    if prev_month != month or prev_day != day:
        remove_deployment(prev_month, prev_day)
except:
    print('Unable to find currently active deployments')


with open('caddy.json', 'r') as f:
    json = f.read()

json = json.replace('<BOLT_PORT>', bolt_port)
json = json.replace('<HTTP_PORT>', gui_port)
json = json.replace('<PREV_BOLT_PORT>', current_active_bolt_port)
json = json.replace('<PREV_HTTP_PORT>', current_active_http_port)

# Update config
requests.post('http://localhost:2019/load', json, headers={'Content-Type': 'application/json'})
# print(response.json())

























# from create_db import logging, root, dump_dir, date, create
# import paramiko
# from scp import SCPClient
# import os
# import json
# import docker
# import arrow
# import requests
# import json

# NEO4J_VERSION = '5.16.0'

# today = arrow.utcnow()

# with open('autodeploy.config.json', 'r') as f:
#     conf = json.load(f)

# logging.warning('starting ssh connection')
# ssh = paramiko.SSHClient()
# ssh.load_system_host_keys()
# ssh.connect(conf['server'], username=conf['user'])

# logging.warning('making the directory for target date')
# dest = os.path.join(conf['target_dir'], f'{today.year}/{today.month:02d}/{today.day:02d}', '')
# ssh.exec_command(f'mkdir -p {dest}')

# logging.warning('sending dump via scp')   
# with SCPClient(ssh.get_transport()) as scp:
#     scp.put(dump_dir, recursive=True, remote_path=dest)


