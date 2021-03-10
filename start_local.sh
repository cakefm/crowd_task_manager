export TROMPA_BACKEND_MODE=TEST
export TROMPA_API_PORT=8888
docker-compose down --volumes && docker-compose build --force-rm trompa && docker-compose up