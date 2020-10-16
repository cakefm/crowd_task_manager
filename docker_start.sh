echo "Starting modules..."
./run_modules.sh
echo "Waiting for port 443 to open..."
while ! nc -z localhost 443; do   
  sleep 0.1 
done
echo "Sending PDF..."
curl -F file=@/root/crowd-task-manager/testing_resources/pdf/beethoven.pdf -F mei=@/dev/null http://localhost:443/upload

