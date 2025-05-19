#!/bin/bash

mkdir logs
mkdir data

cp example/env .env
cp -r example/projects .

groupadd -g 9999 seekerdog
chown -R 50000:seekerdog *
chmod -R 2775 ./projects
chmod g+s ./projects


docker-compose up -d
