from confluent_kafka import Producer
from fake_web_events import Simulation
import socket
import os
import json 

SIMULATION_TIME_SECONDS = int(float(os.environ.get("SIMULATION_TIME_SECONDS", 60)))
USER_POOL_SIZE = int(float(os.environ.get("USER_POOL_SIZE", 100)))
SESSIONS_PER_DAY = int(float(os.environ.get("SESSIONS_PER_DAY", 100000)))
KAFKA_HOST = os.environ.get("KAFKA_HOST", 'host1:9092,host2:9092')
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", 'mytopic')

def createKafkaProducer():
    conf = {'bootstrap.servers': KAFKA_HOST,
            'client.id': socket.gethostname()}

    producer = Producer(conf)
    return producer

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))
def runSimulation():
    simulation = Simulation(user_pool_size=USER_POOL_SIZE, sessions_per_day=SESSIONS_PER_DAY)
    events = simulation.run(duration_seconds=SIMULATION_TIME_SECONDS)
    producer = createKafkaProducer()
    for event in events:
        payload = json.dumps(event)
        producer.produce(KAFKA_TOPIC, key="key", value=payload, callback=acked)
        producer.poll(0)

if __name__ == '__main__':
    runSimulation()

