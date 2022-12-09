import importlib
import os
import json
import logging
import shutil
import sys
import arrow
import docker

from time import sleep

NEO4J_VERSION = '5.1.0'

today =  arrow.utcnow()
date =  f'{today.year}-{today.month:02d}-{today.day:02d}'

root = '/home/romain/Projects/perso/internet-yellow-pages/'
tmp_dir = f'{root}neo4j/tmp/{date}/'
dump_dir = f'{root}/dumps/{today.year}/{today.month:02d}/{today.day:02d}/'

os.makedirs(tmp_dir, exist_ok=True)
os.makedirs(dump_dir, exist_ok=True)

# Initialize logging
scriptname = sys.argv[0].replace('/','_')[0:-3]
FORMAT = '%(asctime)s %(processName)s %(message)s'
logging.basicConfig(
        format=FORMAT, 
        filename=f'{dump_dir}iyp-{date}.log',
        level=logging.WARNING, 
        datefmt='%Y-%m-%d %H:%M:%S'
        )
logging.warning("Started: %s" % sys.argv)

# Load configuration file
with open('config.json', 'r') as fp:
    conf = json.load(fp)

# Start a new neo4j container
client = docker.from_env()

# docker run -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/password -e NEO4J_server_memory_heap_initial__size=1G -e
# NEO4J_server_memory_heap_max__size=8G  -v /home/romain/Projects/perso/internet-yellow-pages/neo4j/test1/data:/data --name iyp-2020-12-06 neo4j 


# Start a new docker image
logging.warning('Starting new container...')
container = client.containers.run(
        'neo4j:'+NEO4J_VERSION, 
        name = f'iyp-{date}',
        ports = {
            7474: 7474,
            7687: 7687
            },
        volumes = {
            tmp_dir: {'bind': '/data', 'mode': 'rw'}, 
            },
        environment = {
            'NEO4J_AUTH': 'neo4j/password',
            'NEO4J_server_memory_heap_initial__size': '16G',
            'NEO4J_server_memory_heap_max__size': '16G',
            },
        remove = True,
        detach=True
    )

# Wait for the container to be ready
timeout = 120
stop_time = 3
elapsed_time = 0
while client.containers.get(container.id).status != 'running' and elapsed_time < timeout:
    sleep(stop_time)
    elapsed_time += stop_time
    #container.reload()
    continue

# Fetch data and feed to neo4j 
logging.warning('Fetching data...')
status = {}
no_error = True
for module_name in conf['iyp']['crawlers']:
    module = importlib.import_module(module_name)

    try:
        print(module)
        logging.warning(f'start {module}')
        crawler = module.Crawler(module.ORG, module.URL)
        crawler.run()
        crawler.close()
        status[module_name] = "OK"
        logging.warning(f'end {module}')

    except Exception as e:
        no_error = False
        logging.error('crawler crashed!!\n')
        logging.error(e)
        logging.error('\n')
        status[module_name] = e



# Stop container
logging.warning('Stopping container...')
container.stop(timeout=180)

# Dump the database
#docker run --interactive --tty --rm \
#   --volume=$HOME/neo4j/data:/data \  
#   --volume=$HOME/neo4j/backups:/backups \  
#   neo4j/neo4j-admin:5.2.0 \
#neo4j-admin database dump neo4j --to-path=/backups

logging.warning('Dumping database...')
if os.path.exists(f'{dump_dir}/neo4j.dump'):
    os.remove(f'{dump_dir}/neo4j.dump')

# make sure the directory is writable for any user
os.chmod(dump_dir, 0o777)

container = client.containers.run(
    'neo4j/neo4j-admin:'+NEO4J_VERSION,
    command = 'neo4j-admin database dump neo4j --to-path=/dumps --verbose',
    tty = True,
    stdin_open = True,
    remove = True,
    volumes = {
        tmp_dir: {'bind': '/data', 'mode': 'rw'}, 
        dump_dir: {'bind': '/dumps', 'mode': 'rw'}, 
        }
)

# rename dump
os.rename(f'{dump_dir}/neo4j.dump', f'{dump_dir}/iyp-{date}.dump')


if not no_error:
    # TODO send an email
    print('there was errors!')
    print(status)
    pass
else:
    shutil.rmtree(tmp_dir)
    pass
logging.warning("Finished: %s" % sys.argv)
