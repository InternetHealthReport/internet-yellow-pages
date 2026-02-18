import argparse
import importlib
import json
import logging
import os
import subprocess as sp
import sys
from datetime import datetime, timezone
from shutil import rmtree
from time import sleep

import docker
import paramiko
from scp import SCPClient

NEO4J_VERSION = '5.26.17'
NEO4J_ADMIN_VERSION = '2025-community-debian'

STATUS_OK = 'OK'


def log_commit_info():
    try:
        tag = sp.run(['git', 'tag', '--points-at=HEAD'], capture_output=True, text=True)
        tag.check_returncode()
    except sp.CalledProcessError as e:
        logging.warning(e)
        logging.warning(tag.stderr.strip())
        return
    try:
        commit_info = sp.run(['git', 'show', '--no-patch', '--format=%H %cI'], capture_output=True, text=True)
        commit_info.check_returncode()
    except sp.CalledProcessError as e:
        logging.warning(e)
        logging.warning(commit_info.stderr.strip())
        return
    tag = tag.stdout.strip()
    if not tag:
        tag = 'none'
    commit_hash, commit_timestamp = commit_info.stdout.split()
    logging.info(f'commit:{commit_hash} date:{commit_timestamp} tag:{tag}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--archive', action='store_true', help='push dump to archive server')
    parser.add_argument('-d', '--directory', help='store database in a bind mount instead of a volume')
    args = parser.parse_args()

    today = datetime.now(tz=timezone.utc)
    date = today.strftime('%Y-%m-%d')

    # Use the current directory as root.
    root = os.path.dirname(os.path.realpath(__file__))
    # Alternatively, specify your own path.
    # root = ''
    if not root:
        sys.exit('Please configure a root path.')

    dump_dir = os.path.join(root, 'dumps', today.strftime('%Y/%m/%d'))

    os.makedirs(dump_dir, exist_ok=True)

    # Initialize logging
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        filename=os.path.join(dump_dir, f'iyp-{date}.log'),
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')
    log_commit_info()

    # Load configuration file
    with open('config.json', 'r') as fp:
        conf = json.load(fp)

    # Neo4j container settings
    neo4j_volume = f'data-{date}'
    bind_mount_directory = args.directory
    if bind_mount_directory:
        neo4j_volume = os.path.join(bind_mount_directory, neo4j_volume)
        os.makedirs(neo4j_volume, exist_ok=True)
        logging.info(f'Using bind mount: {neo4j_volume}')

    # Start a new neo4j container
    client = docker.from_env()

    # ######### Start a new docker image ##########

    auth = 'none'
    if 'login' in conf['neo4j'] and 'password' in conf['neo4j']:
        auth = f'{conf["neo4j"]["login"]}/{conf["neo4j"]["password"]}'

    logging.info('Starting new container...')
    container = client.containers.run(
        'neo4j:' + NEO4J_VERSION,
        name=f'iyp-{date}',
        ports={
            7687: conf['neo4j']['port']
        },
        volumes={
            neo4j_volume: {'bind': '/data', 'mode': 'rw'},
        },
        environment={
            'NEO4J_AUTH': auth,
            'NEO4J_server_memory_heap_initial__size': '16G',
            'NEO4J_server_memory_heap_max__size': '16G',
        },
        user=os.getuid(),
        remove=True,
        detach=True
    )

    # Wait for the container to be ready
    timeout = 300
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
            logging.info('Container ready.')
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

    # ########## Fetch data and feed to neo4j ##########

    class RelationCountError(Exception):
        def __init__(self, message):
            self.message = message
            super().__init__(self.message)

    logging.info('Fetching data...')
    status = {}
    no_error = True
    for module_name in conf['iyp']['crawlers']:
        crawler = None
        try:
            module = importlib.import_module(module_name)
            logging.info(f'start {module}')
            name = module_name.replace('iyp.crawlers.', '')
            crawler = module.Crawler(module.ORG, module.URL, name)
            crawler.run()
            passed = crawler.unit_test()
            if not passed:
                error_message = f'Did not receive data from crawler {name}'
                raise RelationCountError(error_message)
            status[module_name] = STATUS_OK
        except RelationCountError as relation_count_error:
            no_error = False
            logging.error(relation_count_error)
            status[module_name] = relation_count_error
        except Exception as e:
            no_error = False
            logging.error('Crawler crashed!')
            logging.error(e)
            status[module_name] = e
        finally:
            if crawler is not None:
                try:
                    crawler.close()
                except Exception as cleanup_error:
                    logging.error(f'Failed to close crawler: {cleanup_error}')
        logging.info(f'end {module}')

    # ######### Post processing scripts ##########

    logging.info('Post-processing...')
    for module_name in conf['iyp']['post']:
        post = None
        try:
            module = importlib.import_module(module_name)
            name = module_name.replace('iyp.post.', '')
            logging.info(f'start {module}')
            post = module.PostProcess(name)
            post.run()
            status[module_name] = STATUS_OK
        except Exception as e:
            no_error = False
            logging.error('Crawler crashed!')
            logging.error(e)
            status[module_name] = e
        finally:
            if post is not None:
                try:
                    post.close()
                except Exception as cleanup_error:
                    logging.error(f'Failed to close post-processor: {cleanup_error}')
        logging.info(f'end {module}')

    # ######### Stop container and dump DB ##########

    logging.info('Stopping container...')
    container.stop(timeout=1800)

    logging.info('Dumping database...')
    dump_file = os.path.join(dump_dir, 'neo4j.dump')
    if os.path.exists(dump_file):
        os.remove(dump_file)

    # make sure the directory is writable for any user
    os.chmod(dump_dir, 0o777)

    container = client.containers.run(
        'neo4j/neo4j-admin:' + NEO4J_ADMIN_VERSION,
        command='neo4j-admin database dump neo4j --to-path=/dumps --verbose',
        tty=True,
        stdin_open=True,
        remove=True,
        volumes={
            neo4j_volume: {'bind': '/data', 'mode': 'rw'},
            dump_dir: {'bind': '/dumps', 'mode': 'rw'},
        },
        user=os.getuid()
    )

    # Delete the data volume once the dump been created
    if bind_mount_directory:
        rmtree(neo4j_volume)
    else:
        client.volumes.get(neo4j_volume).remove()

    # rename dump

    os.rename(dump_file, os.path.join(dump_dir, f'iyp-{date}.dump'))

    if not no_error:
        final_words = '\nErrors: '
        for module, status in status.items():
            if status != STATUS_OK:
                final_words += f'\n{module}: {status}'
    else:
        final_words = 'No error :)'
    # Delete tmp file in cron job
    #    shutil.rmtree(tmp_dir)

    logging.info(f'Finished: {sys.argv} {final_words}')
    if not no_error:
        # Add the log line to indicate to autodeploy that there were errors.
        logging.error('There were errors!')

    if args.archive:
        # Push the dump and log to ihr archive
        ssh = paramiko.SSHClient()
        # Do not show info logging for paramiko.
        logging.getLogger('paramiko').setLevel(logging.WARNING)
        ssh.load_system_host_keys()
        ssh.connect(conf['archive']['host'], username=conf['archive']['user'])

        dest = os.path.join(conf['archive']['base_path'], today.strftime('%Y/%m/%d'))
        ssh.exec_command(f'mkdir -p {dest}')

        with SCPClient(ssh.get_transport()) as scp:
            scp.put(dump_dir, recursive=True, remote_path=dest)


if __name__ == '__main__':
    main()
