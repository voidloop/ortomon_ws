import functools
import logging

from paho.mqtt import client as mqtt
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
import automationhat
import json
import configparser

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('ortomon_ws')


def task(client: mqtt.Client, topic: str, wind_conversion_factor: float, direction_conversion_factor: float):
    # don't allow negative values
    wind_v = max(0.0, automationhat.analog.one.read())
    direction_v = max(0.0, automationhat.analog.two.read())
    log.debug('Vout: wind={}, direction={}'.format(wind_v, direction_v))

    data = dict(wind=int(wind_v * wind_conversion_factor),
                direction=int(direction_v * direction_conversion_factor))

    client.publish(topic, json.dumps(data))
    log.info('Message published: topic={}, data={}'.format(topic, data))


def on_connect(scheduler: BackgroundScheduler, config: dict, client, _userdata, _flags, rc):
    if rc == 0:
        log.info("Connected to MQTT broker")
        fn = functools.partial(task, client, config['mqtt']['topic'],
                               float(config['wind']['conversion_factor']),
                               float(config['direction']['conversion_factor']))
        scheduler.add_job(fn, 'interval', seconds=int(config['mqtt']['interval']), name='publisher')
        scheduler.start()
    else:
        log.error("Failed to connect, return code %d\n", rc)


def connect_mqtt(scheduler: BackgroundScheduler) -> mqtt.Client:
    config_dir = Path.home().joinpath('.ortomon_ws')
    config = configparser.ConfigParser()
    config.read(config_dir.joinpath('config.ini'))

    client = mqtt.Client(config['mqtt']['client_id'])
    client.tls_set(ca_certs=config['mqtt']['ca_certs'],
                   certfile=config['mqtt']['cert_file'],
                   keyfile=config['mqtt']['key_file'])
    client.tls_insecure_set(False)

    client.on_connect = functools.partial(on_connect, scheduler, config)
    client.connect(config['mqtt']['host'], int(config['mqtt']['port']))

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
