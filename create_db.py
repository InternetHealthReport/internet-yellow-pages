import importlib
import os
import json
import logging
import shutil
import sys
import arrow
import docker

from time import sleep
from send_email import send_email

NEO4J_VERSION = '5.1.0'

today =  arrow.utcnow()
date =  f'{today.year}-{today.month:02d}-{today.day:02d}'

# Uncomment the line below to use the current directory as root.
# Alternatively, specify your own path.
# root = os.path.dirname(os.path.realpath(__file__))
root = ''
if not root:
    sys.exit('Please configure a root path.')
if not root.endswith('/'):
    root += '/'
tmp_dir = f'{root}neo4j/tmp/{date}/'
dump_dir = f'{root}dumps/{today.year}/{today.month:02d}/{today.day:02d}/'

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

########## Start a new docker image ##########

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
stop_time = 1
elapsed_time = 0
container_ready = False

while elapsed_time < timeout:
    sleep(stop_time)
    elapsed_time += stop_time
    # Not the most premium solution, but the alternative is using
    # stream=True, which creates a blocking generator that we have
    # to somehow interrupt in case the database does not start
    # correctly. And writing a signal handler just for this seems
    # overkill.
    last_msg = container.logs(stderr=False, tail=1)
    if last_msg.endswith(b'Started.\n'):
        logging.warning('Container ready.')
        container_ready = True
        break

if not container_ready:
    logging.error('Timed our while waiting for container to start.')
    try:
        container_logs = container.logs().decode('utf-8')
    except Exception as e:
        logging.error(f'Can not get logs from container: {e}')
        sys.exit('Problem while starting the container.')
    logging.error(f'Container logs:\n{container_logs}')
    logging.error('Trying to stop container...')
    container.stop()
    sys.exit('Problem while starting the container.')

########## Fetch data and feed to neo4j ##########

logging.warning('Fetching data...')
status = {}
no_error = True
for module_name in conf['iyp']['crawlers']:
    module = importlib.import_module(module_name)

    try:
        logging.warning(f'start {module}')
        name = module_name.replace('iyp.crawlers.', '')
        crawler = module.Crawler(module.ORG, module.URL, name)
        crawler.run()
        crawler.close()
        status[module_name] = "OK"
        logging.warning(f'end {module}')

    except Exception as e:
        no_error = False
        logging.exception('crawler crashed!!')
        status[module_name] = e
        send_email(e)


########## Post processing scripts ##########

logging.warning('Post-processing...')
for module_name in conf['iyp']['post']:
    module = importlib.import_module(module_name)

    try:
        logging.warning(f'start {module}')
        post = module.PostProcess()
        post.run()
        post.close()
        status[module_name] = "OK"
        logging.warning(f'end {module}')

    except Exception as e:
        no_error = False
        logging.error('crawler crashed!!\n')
        logging.error(e)
        logging.error('\n')
        status[module_name] = e


########## Stop container and dump DB ##########

logging.warning('Stopping container...')
container.stop(timeout=180)

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

final_words = ''
if not no_error:
    # TODO send an email
    final_words += 'There was errors!'
    logging.error('there was errors!\n')
    logging.error({k:error for k, error in status.items() if error!='OK'})
else:
    final_words = 'No error :)'
# Delete tmp file in cron job
#    shutil.rmtree(tmp_dir)

logging.warning(f"Finished: {sys.argv} {final_words}")
