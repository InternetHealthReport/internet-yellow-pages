# Resources for IYP public instance

To run the public instance copy the apache host configuration file (000-default.conf) to ```/etc/apache2/sites-enabled/```.
Then copy the html files (guides) from the public/www folder to /var/www/iyp:
```
sudo cp -r public/www /var/www/iyp
```

And restart apache:
```
sudo systemctl restart apache2
```

## Apache configuration
The ./000-default.conf file contains the apache virtual host configuration for running the public instance.
It redirects queries to iyp.iijlab.net/ to iyp.iijlab.net/iyp/ which is an internal proxy for port 7474.

## TLS configuration
- Create a certificate with let's encrypt (https://www.digitalocean.com/community/tutorials/how-to-secure-apache-with-let-s-encrypt-on-ubuntu-20-04)
- Then copy certificates in the folder structure expected by neo4j:
```
cp public
sh copy_certificates.sh
```
