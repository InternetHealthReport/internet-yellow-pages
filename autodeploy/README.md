# Autodeployment Script

## Usage
### Starting caddy
Make sure that Caddy is running. If not, run it with `docker compose up caddy`. 
If Caddy was running previously, then the new Caddy instance will resume from 
the previous config. See the [Caddy docs](https://caddyserver.com/docs/running#docker-compose) 
for more info.

### Running the script
To run the script, run `python3 -m autodeploy`. This will first find the date
of the most recent active deployment using the caddy config. If there is no
active deployment, today's date is used. With this date, the script will then 
check ihr-archive to see if a dump has been pushed in the subsequent 7 days. If 
so, a neo4j instance will be deployed using that dump. For example, if the latest
deployment is for 2024-06-15, the script will check if there is a dump for 
2024-06-16, 2024-06-17,..., 2024-06-22.

Alternatively, running `python3 -m autodeploy [year]-[month]-[day]` will check 
if there is a dump in the archive for the specified date and deploy it directly.

## How it works

### Checking for a dump to deploy
If the date is not provided when running the script, it will first make a request
to Caddy to get the current config. The config is parsed to retrieve the port of the
active database. The date is parsed from the port number as explained below. Starting
from this date, the next 7 days are then checked in ihr-archive for valid dumps.

#### Caddy Config
Caddy is updated by substituting the desired ports in `caddy.template.json`. The ports
are constructed with the following structure: 1MMDD for neo4j http port, and 2MMDD for neo4j
bolt port. The json is sent to caddy by making a POST request to sandbox.ihr.live:2019/load.
The current config is retrieved by making a GET request to sandbox.ihr.live:2019/config.

### Starting the database
Once a dump has been found, its log is downloaded from the archive. If the log indicates
that there are no errors, then the dump is downloaded. A docker container is then started
that loads the dump into a neo4j database. The database is stored in a docker volume with
the name data-MM-DD. Another container is then used to start the database using the data
stored in data-MM-YY. It binds its internal neo4j 7474 and 7687 ports to the external
ones that contain the dump's date.

If a container is already running for this date, it and its data volume are deleted, and
a new one is created from the downloaded dump data.

If there was already an active database, it becomes the previous database. The current
previous database container is stopped, and its data volume is deleted.


