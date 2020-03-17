import pika
import pathlib
import yaml

from pymongo import MongoClient

with open("../settings.yaml", "r") as file:
    config = yaml.safe_load(file.read())


mongo_address = re.search('[a-zA-Z]+:[0-9]+', config["mongo_server"]).group(0)
rabbitmq_address = config["rabbitmq_address"]
project_queue_name = config["mq_project_queue"]
project_collection_name = config["mongo_project_collection"]

address = rabbitmq_address.split(":")

connection = pika.BlockingConnection(
    pika.ConnectionParameters(address[0], address[1]))
channel = connection.channel()
channel.exchange_declare(exchange='project_initiators', exchange_type='direct')
# channel.queue_declare(queue=project_queue_name)
result = channel.queue_declare(queue=project_queue_name, exclusive=True)
queue_name = result.method.queue

for project in projects:
    channel.queue_bind(exchange='project_initiators', queue=queue_name,
                       routing_key=project)


def callback(ch, method, properties, body):
    if method.routing_key == 'omr':
        print("Initializing OMR Project")


channel.basic_consume(queue=project_queue_name, on_message_callback=callback, auto_ack=True)

print('Project manager is listening for new projects...')
channel.start_consuming()
