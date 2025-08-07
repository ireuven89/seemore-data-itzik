FROM node:18-slim


WORKDIR app
COPY ./package-lock.json .
COPY ./package.json .

ENV

RUN npm install .