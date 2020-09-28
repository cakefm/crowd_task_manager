FROM ubuntu:18.04
RUN apt-get update
RUN apt-get install -y python3 python3-pip
COPY ./requirements.txt /root/crowd-task-manager/requirements.txt
RUN pip3 install -r $HOME/crowd-task-manager/requirements.txt
RUN apt-get install -y screen
COPY . /root/crowd-task-manager
COPY ./settings_docker.yaml /root/crowd-task-manager/settings.yaml
CMD cd /root/crowd-task-manager/api && python3 api.py
