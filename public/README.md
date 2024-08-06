# IYP public instance
IYP is served behind a NGINX reverse proxy. 
The [landing page](https://github.com/InternetHealthReport/iyp-website/) is at the root and the `iyp/` path is pointing to the neo4j instance (`public_notls`).

Example nginx configuration:
```
location ^~ /iyp/ {
   proxy_pass http://10.255.255.11:7474/;
 }
```

The BOLT endpoint (iyp-bolt.iijlab.net) points directly to Neo4j's 7687 port.

# Note
The `public_tls` configuration is not currently used. This is required if the machine serving IYP is publicly accessible and directly accessed by clients.
