#!/bin/bash
sudo pkill node || true
cd ..
cd shopping-stories/website
git checkout preprod
git pull
npm install
npm run build && npm run start &
disown