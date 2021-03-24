export TROMPA_BACKEND_MODE=LIVE
export TROMPA_API_PORT=8888
if [ ! -f settings_docker_live.yaml ]; then
    cp settings_docker_test.yaml settings_docker_live.yaml
fi
rm -R ./logs
docker-compose down --volumes && docker-compose build --force-rm trompa && docker-compose up