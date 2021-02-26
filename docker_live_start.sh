echo "Starting modules..."
./docker_live_run_modules.sh
echo "Waiting for port 443 to open..."
while ! nc -z localhost 443:
do   
sleep 0.1 
done

# Module to view logs of after start-up
screen -r omr_planner