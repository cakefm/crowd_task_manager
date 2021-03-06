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
    "post_processing/post_processing_mq.py"
    "api/api.py"
    "task_scheduler/task_passthrough.py"
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


echo "Waiting for port ${API_PORT} to open..."
while ! nc -z localhost ${API_PORT}:
do   
sleep 0.1 
done
echo "Sending PDF..."
curl -F file=@/root/crowd-task-manager/testing_resources/pdf/beethoven_orchestra_p1.pdf -F "mei=@/dev/null;filename=" http://localhost:${API_PORT}/upload
echo "PDF sent succesfully!"

# Module to view screen of after start-up
screen -r task_scheduler_new