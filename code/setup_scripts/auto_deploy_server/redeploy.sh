#!/bin/bash
sudo service nginx stop ||  true
sudo pkill gunicorn || true
cd ..
cd shoppingstories-parser/code
git checkout .
git pull
chmod +x ./setup_scripts/prod_setup.sh
./setup_scripts/prod_setup.sh
disown