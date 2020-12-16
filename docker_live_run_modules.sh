#!/bin/sh

# start modules
if ! screen -list | grep -q "omr_planner"; then
    echo 'starting omr_planner'
    cd $HOME/crowd-task-manager/project_manager
    screen -L -Logfile screen_log -dm -S omr_planner python3 omr_planner.py
fi

if ! screen -list | grep -q "ce_comm"; then
    echo 'starting ce_comm'
    cd $HOME/crowd-task-manager/ce_integration
    screen -L -Logfile screen_log -dm -S ce_communicator python3 ce_communicator.py
fi

if ! screen -list | grep -q "pdf2mei"; then
    echo 'starting pdf2mei'
    cd $HOME/crowd-task-manager/pdf_to_mei
    screen -L -Logfile screen_log -dm -S pdf2mei python3 pdf_to_mei.py
fi

if ! screen -list | grep -q "aligner"; then
    echo 'starting aligner'
    cd $HOME/crowd-task-manager/aligner
    screen -L -Logfile screen_log -dm -S aligner python3 aligner_mq.py
fi

if ! screen -list | grep -q "slicer"; then
    echo 'starting slicer'
    cd $HOME/crowd-task-manager/slicer
    screen -L -Logfile screen_log -dm -S slicer python3 slicer_mq.py
fi

if ! screen -list | grep -q "task_scheduler"; then
    echo 'starting task_scheduler'
    cd $HOME/crowd-task-manager/task_scheduler
    screen -L -Logfile screen_log -L -Logfile screen_log -dm -S task_scheduler python3 task_scheduler_new.py
fi

if ! screen -list | grep -q "aggregator_xml"; then
    echo 'starting xml aggregator'
    cd $HOME/crowd-task-manager/aggregator
    screen -L -Logfile screen_log_xml -dm -S aggregator_xml python3 aggregator_xml_mq.py
fi

if ! screen -list | grep -q "aggregator_form"; then
    echo 'starting form aggregator'
    cd $HOME/crowd-task-manager/aggregator
    screen -L -Logfile screen_log_form -dm -S aggregator_form python3 aggregator_form_mq.py
fi


if ! screen -list | grep -q "score_rebuilder"; then
    echo 'starting score rebuilder'
    cd $HOME/crowd-task-manager/score_rebuilder
    screen -L -Logfile screen_log -dm -S score_rebuilder python3 score_rebuilder_mq.py
fi

if ! screen -list | grep -q "form_processor"; then
    echo 'starting form processor'
    cd $HOME/crowd-task-manager/form_processor
    screen -L -Logfile screen_log -dm -S form_processor python3 form_processor_mq.py
fi

if ! screen -list | grep -q "github_init"; then
    echo 'starting github_init'
    cd $HOME/crowd-task-manager/github
    screen -L -Logfile screen_log_init -dm -S github_init python3 github_init_mq.py
fi

if ! screen -list | grep -q "github_update"; then
    echo 'starting github_update'
    cd $HOME/crowd-task-manager/github
    screen -L -Logfile screen_log_update -dm -S github_update python3 github_update_mq.py
fi

if ! screen -list | grep -q "flask"; then
     echo 'starting flask api.py'
     cd $HOME/crowd-task-manager/api
     screen -L -Logfile screen_log -dm -S flask python3 api.py
fi
