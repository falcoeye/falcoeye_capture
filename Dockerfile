FROM ubuntu:20.04

MAINTAINER "falcoeye team"

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install -y python3
RUN apt install  -y python3-pip
RUN DEBIAN_FRONTEND="noninteractive" apt-get install --yes python3-opencv
#RUN apt-get install -y git && \
RUN apt-get -y install ffmpeg libsm6 libxext6 
RUN pip3 install --no-cache-dir -r requirements.txt 

COPY . .
