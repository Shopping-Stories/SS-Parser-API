# Instructions to deploy auto-deploy server
1. Look at prod_setup.sh
2. Use the same method as in prod_setup.sh to deploy this server
3. When you install gunicorn rename it to depicorn so it can't get pkilled by redeploy.sh
4. Run on socket unix:/tmp/deployment.sock