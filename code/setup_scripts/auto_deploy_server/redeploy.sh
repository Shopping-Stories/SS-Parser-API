#!/bin/bash
sudo service nginx stop ||  true
sudo pkill gunicorn || true
cd /home/admin/shoppingstories-parser/code
sudo git fetch
sudo git checkout api
sudo git reset --hard
sudo git pull
sudo chmod +x ./setup_scripts/prod_setup.sh
./setup_scripts/prod_setup.sh
disown