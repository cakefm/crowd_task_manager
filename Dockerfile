FROM ubuntu:18.04

# Install deps
RUN apt-get clean
RUN apt-get update --fix-missing
RUN apt-get install -y python3 python3-pip
RUN apt-get install -y screen
RUN apt-get install -y curl
RUN apt-get install -y netcat
RUN apt-get install -y poppler-utils

# Install python deps
COPY ./requirements.txt /root/crowd-task-manager/requirements.txt
RUN pip3 install -r $HOME/crowd-task-manager/requirements.txt

# Copy over project
COPY . /root/crowd-task-manager
COPY ./settings_docker_test.yaml /root/crowd-task-manager/settings.yaml
RUN mkdir /omr_files
WORKDIR /root/crowd-task-manager/

# Add docker-compose-wait tool -------------------
ENV WAIT_VERSION 2.7.2
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait ./wait
RUN chmod +x ./wait