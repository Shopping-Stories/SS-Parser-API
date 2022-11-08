#!/bin/bash
sudo service nginx stop ||  true
sudo pkill gunicorn || true
cd ..
cd shoppingstories-parser/code
sudo git checkout .
sudo git pull
sudo chmod +x ./setup_scripts/prod_setup.sh
./setup_scripts/prod_setup.sh
disown