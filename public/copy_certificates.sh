# Change group of all letsencrypt files to neo4j (7474)
#sudo chgrp -R 7474 /etc/letsencrypt/* # Make sure all directories and files are group readable.
sudo chmod -R g+rx /etc/letsencrypt/*

sudo rm -rf certificates
sudo mkdir certificates
cd certificates

sudo mkdir bolt
sudo mkdir https

export MY_DOMAIN=iyp.iijlab.net

for certsource in bolt https ; do
   sudo cp /etc/letsencrypt/live/$MY_DOMAIN/fullchain.pem $certsource/neo4j.cert
   sudo cp /etc/letsencrypt/live/$MY_DOMAIN/privkey.pem $certsource/neo4j.key
   sudo mkdir $certsource/trusted
   sudo cp /etc/letsencrypt/live/$MY_DOMAIN/fullchain.pem $certsource/trusted/neo4j.cert ;
done

# Finally make sure everything is readable to the database
sudo chown -R $(id -u):$(id -g) *
# FIXME there should be a better way to do this..
sudo chmod -R a+r *
