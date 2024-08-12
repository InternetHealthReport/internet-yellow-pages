# Autodeployment Script

## Usage

### Starting caddy
Make sure that Caddy is running. If not, run it with `docker compose up caddy`

### Autodeploy
To run the script, run `python3 -m autodeploy`. This will search 
ihr-archive to see if a dump has been pushed in the last week. If 
so, a neo4j instance will be deployed using that dump. Alternatively, 
running `python3 -m autodeploy [year]-[month]-[day]` will check if 
there is a dump in the archive for the specified date and deploy it directly.