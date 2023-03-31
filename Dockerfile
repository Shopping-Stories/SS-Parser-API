# Build this file with:
# docker build --build-arg AWS_SECRET_ACCESS_KEY=<insert> --build-arg MONGO_LOGIN_STRING=<insert> --build-arg AWS_ACCESS_KEY_ID=<insert> -t shoppingstories/parser -f Dockerfile .
FROM bitnami/git
WORKDIR /gitapp
# Cause this image to need rebuilding if the SS-Parser-API github is updated
# ADD "https://api.github.com/repos/Shopping-Stories/SS-Parser-API/commits?per_page=1" latest_commit
ARG CACHEBUST=1
RUN echo "$CACHEBUST"
RUN git clone --depth 1 https://github.com/Shopping-Stories/SS-Parser-API.git

FROM python:3.10
WORKDIR /parser
ADD ".env" "/parser/.env"
ARG AWS_SECRET_ACCESS_KEY
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ARG MONGO_LOGIN_STRING
ENV MONGO_LOGIN_STRING=$MONGO_LOGIN_STRING
ARG AWS_ACCESS_KEY_ID
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
COPY --from=0 /gitapp/SS-Parser-API/code/parser_requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r parser_requirements.txt
COPY --from=0 /gitapp/SS-Parser-API/code/ ./
COPY ".env" "/parser/api/ssParser/.env"
CMD ["python", "api_entry.py", "parser"]