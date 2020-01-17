# Trell Data Pipeline

A small async pipeline that pipes data from an ingress stream into another
source. Currently it is only compatible with the chirpstack to Kafka.

Current implementation can only parse and produce Elsys payload or raw data into
a Kafka cluster. However other sources and destinations should be easy to
implement by creating a new decoder and/or producer.

## Installation

1. Clone this repo.
2. Create and load a virtual enviroment with python3.5 or above.
3. pip install -r requirements.txt


## Quick start

__Note__: The development config uses a mock producer. To use Kafka either change
the development settings or create a local config with kafka as driver.
A Kafka server must be started to use the kafka producer.

``` shell
python run.py --config-file=config/development.yml
```

## Dependencies

* Python 3.6+
* Kafka 2.0+ (if Kafka producer is used)

## Development

Hack and slash method! No tests to consider yet :(

__Note__: The debug log for the various packages are disabled per default (set to
WARNING level). To enable it, in \__init__.py in init_logging(), change the log
levels to desired level.

## Future work

* Better error handling for different connection problems in the MQTT client.
* Create support for dual manufacturers of sensors and gateways in the same
installation.
* Create a AMQP (RabbitMQ) producer.
* Move production settings into the provisioning/configurtion step instead of
having it inside the repo. I.e. write an ansible template.
* Maybe some tests for the drivers?
