#!/bin/bash
sudo pkill node || true
cd ..
cd shopping-stories/website
sudo git checkout preprod
sudo git reset --hard
sudo git pull
sudo npm install
sleep 1
npm run build
sleep 3
npm run start > /dev/null 2>&1 &
disown