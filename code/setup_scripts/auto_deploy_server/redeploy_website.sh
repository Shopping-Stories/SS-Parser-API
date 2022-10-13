#!/bin/bash
sudo pkill node || true
cd ..
cd shopping-stories/website
git checkout .
git pull
mv package_linux.json package.json
npm install
npm run build
npm run start &
disown