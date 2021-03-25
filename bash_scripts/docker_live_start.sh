#!/bin/bash

echo "Starting modules..."

declare -a modules=(
    "project_manager/omr_planner.py"
    "pdf_to_mei/pdf_to_mei.py" 
    "aligner/aligner_mq.py"
    "slicer/slicer_mq.py"
    "task_scheduler/task_scheduler_new.py"
    "aggregator/aggregator_xml_mq.py"
    "aggregator/aggregator_form_mq.py"
    "score_rebuilder/score_rebuilder_mq.py"
    "form_processor/form_processor_mq.py"
    "github/github_init_mq.py"
    "github/github_update_mq.py"
    "post_processing/post_processing_mq.py"
    "api/api.py"
    "ce_integration/ce_communicator.py"
)

for path in "${modules[@]}"
do
    bname=$(basename "${HOME}/crowd-task-manager/${path}")
    dname=$(dirname "${HOME}/crowd-task-manager/${path}")
    stem=$(echo $bname | cut -d. -f1)
    echo Starting $stem...
    cd $dname
    screen -L -Logfile "/logs/${bname}.log" -dm -S $stem python3 $bname
done

# Module to view screen of after start-up
screen -r omr_planner