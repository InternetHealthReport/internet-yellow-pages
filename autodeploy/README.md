# Autodeployment Script

## Usage

### Starting caddy
Make sure that Caddy is running. If not, run it with `docker compose up caddy`

### Autodeploy
To run the script, run `python3 -m autodeploy`. This will first find the date
of the most recent active deployment using the caddy config. If there is no
active deployment, today's date is used. With this date, the script will then  
check ihr-archive to see if a dump has been pushed in the subsequent 7 days. If 
so, a neo4j instance will be deployed using that dump. Alternatively, 
running `python3 -m autodeploy [year]-[month]-[day]` will check if 
there is a dump in the archive for the specified date and deploy it directly.
