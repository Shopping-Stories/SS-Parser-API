#!/bin/bash
cd setup_scripts
python3 -m venv ../parserEnv
source ../parserEnv/Scripts/activate
pip install -r ../requirements.txt
python ./setup.py
sudo apt install curl gnupg2 ca-certificates lsb-release ubuntu-keyring
curl https://nginx.org/keys/nginx_signing.key | gpg --dearmor \
    | sudo tee /usr/share/keyrings/nginx-archive-keyring.gpg >/dev/null 
if ! gpg --dry-run --quiet --import --import-options import-show /usr/share/keyrings/nginx-archive-keyring.gpg | grep 573BFD6B3D8FBC641079A6ABABF5BD827BD9BF62; then
    echo "Warning: Nginx fingerprint incorrect, DO NOT PROCEED with install" >&2
fi
echo "deb [signed-by=/usr/share/keyrings/nginx-archive-keyring.gpg] \
http://nginx.org/packages/ubuntu `lsb_release -cs` nginx" \
    | sudo tee /etc/apt/sources.list.d/nginx.list
echo -e "Package: *\nPin: origin nginx.org\nPin: release o=nginx\nPin-Priority: 900\n" \
    | sudo tee /etc/apt/preferences.d/99nginx
sudo apt update
sudo apt install nginx
mkdir -p /usr/local/nginx/conf || true
mkdir -p /etc/nginx || true
mkdir -p  /usr/local/etc/nginx || true
sudo cp ./nginx.conf /etc/nginx/nginx.conf
cp ./nginx.conf  /usr/local/etc/nginx/nginx.conf
cp ./nginx.conf /usr/local/nginx/conf/nginx.conf
sudo chmod +x /usr/bin/nginx
/usr/bin/nginx
nginx -s start
cd ..
gunicorn -w 2 -k "gthread" --threads 2 --forwarded-allow-ips="*" api_entry:incoming