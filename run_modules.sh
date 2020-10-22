#!/bin/sh
# stop running modules
# if screen -list | grep -q "omr_planner"; then
#     echo 'stopping omr_planner'
#     screen -XS omr_planner quit
# fi

# if screen -list | grep -q "ce_comm"; then
#     echo 'stopping ce_comm'
#     screen -XS ce_comm quit
# fi

# if screen -list | grep -q "pdf2mei"; then
#     echo 'stopping pdf2mei'
#     screen -XS pdf2mei quit
# fi

# if screen -list | grep -q "slicer"; then
#     echo 'stopping slicer'
#     screen -XS slicer quit
# fi

# if screen -list | grep -q "task_scheduler"; then
#     echo 'stopping task_scheduler'
#     screen -XS task_scheduler quit
# fi

# if screen -list | grep -q "score_rebuilder"; then
#     echo 'stopping score_rebuilder'
#     screen -XS score_rebuilder quit
# fi

# if screen -list | grep -q "github_init"; then
#     echo 'stopping github_init'
#     screen -XS github_init quit
# fi

# if screen -list | grep -q "github_update"; then
#     echo 'stopping github_update'
#     screen -XS github_update quit
# fi


# start modules
if ! screen -list | grep -q "omr_planner"; then
    echo 'starting omr_planner'
    cd $HOME/crowd-task-manager/project_manager
    screen -dm -S omr_planner python3 omr_planner.py
fi

# if ! screen -list | grep -q "ce_comm"; then
#     echo 'starting ce_comm'
#     cd $HOME/crowd-task-manager/ce_integration
#     screen -dm -S ce_comm bash -c 'python3 ce_communicator.py'
# fi

if ! screen -list | grep -q "pdf2mei"; then
    echo 'starting pdf2mei'
    cd $HOME/crowd-task-manager/pdf_to_mei
    screen -dm -S pdf2mei python3 pdf_to_mei.py
fi

if ! screen -list | grep -q "aligner"; then
    echo 'starting aligner'
    cd $HOME/crowd-task-manager/aligner
    screen -dm -S aligner python3 aligner_mq.py
fi

if ! screen -list | grep -q "slicer"; then
    echo 'starting slicer'
    cd $HOME/crowd-task-manager/slicer
    screen -dm -S slicer python3 slicer_mq.py
fi

if ! screen -list | grep -q "task_scheduler"; then
    echo 'starting task_scheduler'
    cd $HOME/crowd-task-manager/task_scheduler
    screen -dm -S task_scheduler python3 task_scheduler.py
fi

if ! screen -list | grep -q "aggregator"; then
    echo 'starting aggregator'
    cd $HOME/crowd-task-manager/aggregator
    screen -dm -S aggregator python3 aggregator_xml_mq.py
fi

# if ! screen -list | grep -q "score_rebuilder"; then
#     echo 'starting score_rebuilder'
#     cd $HOME/crowd-task-manager/score_rebuilder
#     screen -dm -S score_rebuilder bash -c 'python3 score_rebuilder_mq.py'
# fi

# if ! screen -list | grep -q "github_init"; then
#     echo 'starting github_init'
#     cd $HOME/crowd-task-manager/github
#     screen -dm -S github_init bash -c 'python3 github_init_mq.py'
# fi

# if ! screen -list | grep -q "github_update"; then
#     echo 'starting github_update'
#     cd $HOME/crowd-task-manager/github
#     screen -dm -S github_update bash -c 'python3 github_update_mq.py'
# fi

if ! screen -list | grep -q "flask"; then
     echo 'starting flask api.py'
     cd $HOME/crowd-task-manager/api
     screen -dm -S flask python3 api.py
fi
