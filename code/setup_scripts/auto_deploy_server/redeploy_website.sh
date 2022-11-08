#!/bin/bash
sudo pkill node || true
cd ..
cd shopping-stories/website
sudo git checkout preprod
sudo git pull
sudo npm install
npm run build && npm run start &
disown