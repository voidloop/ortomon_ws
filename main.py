import functools
import random
import logging

from paho.mqtt import client as mqtt
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
# import automationhat
import json
import configparser

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('ortomon_ws')


def task(client: mqtt.Client, topic: str):
    # data = dict(wind=automationhat.analog.one.read(),
    #             direction=0)
    data = dict(wind=random.randint(0, 300),
                direction=random.randint(0, 100))
    client.publish(topic, json.dumps(data))
    log.info("Message published on topic \"{}\"".format(topic))


def on_connect(scheduler: BackgroundScheduler, topic, client, _userdata, _flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker")
        scheduler.add_job(functools.partial(task, client, topic), 'interval', seconds=5, name='publisher')
        scheduler.start()
    else:
        log.error("Failed to connect, return code %d\n", rc)


def connect_mqtt(scheduler: BackgroundScheduler) -> mqtt.Client:
    config_dir = Path.home().joinpath('.ortomon_ws')
    config = configparser.ConfigParser()
    config.read(config_dir.joinpath('config.ini'))

    client = mqtt.Client(config['DEFAULT']['client_id'])
    client.tls_set(ca_certs=config['DEFAULT']['ca_certs'],
                   certfile=config['DEFAULT']['cert_file'],
                   keyfile=config['DEFAULT']['key_file'])
    client.tls_insecure_set(False)

    client.on_connect = functools.partial(on_connect, scheduler, config['DEFAULT']['topic'])
    client.connect(config['DEFAULT']['broker'], int(config['DEFAULT']['port']))

    return client


def run():
    scheduler = BackgroundScheduler()
    client = connect_mqtt(scheduler)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
    finally:
        scheduler.shutdown()


if __name__ == '__main__':
    run()
