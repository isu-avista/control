# get base image
FROM ubuntu:latest

# create working direcotry
WORKDIR ./

RUN apt-get update -y
RUN apt-get install -y apt-utils
RUN apt-get install -y git ssh libatlas-base-dev libpq-dev
RUN apt-get install -y python3 python3-pip python3-dev python3-wheel python3-setuptools python3-venv

# copy requirements
COPY ./requirements.txt ./requirements.txt

# install python dependencies
RUN pip3 install -r requirements.txt

# copy application files
COPY ./ ./