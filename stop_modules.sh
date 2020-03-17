#!/bin/sh
# stop running modules
if screen -list | grep -q "omr_planner"; then
    echo 'stopping omr_planner'
    screen -XS omr_planner quit
fi

if screen -list | grep -q "ce_comm"; then
    echo 'stopping ce_comm'
    screen -XS ce_comm quit
fi

if screen -list | grep -q "pdf2mei"; then
    echo 'stopping pdf2mei'
    screen -XS pdf2mei quit
fi

if screen -list | grep -q "slicer"; then
    echo 'stopping slicer'
    screen -XS slicer quit
fi

if screen -list | grep -q "task_scheduler"; then
    echo 'stopping task_scheduler'
    screen -XS task_scheduler quit
fi

if screen -list | grep -q "score_rebuilder"; then
    echo 'stopping score_rebuilder'
    screen -XS score_rebuilder quit
fi

if screen -list | grep -q "github_init"; then
    echo 'stopping github_init'
    screen -XS github_init quit
fi

if screen -list | grep -q "github_update"; then
    echo 'stopping github_update'
    screen -XS github_update quit
fi

