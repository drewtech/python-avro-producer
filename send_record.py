#!/usr/bin/env python

import json
import uuid

#from confluent_kafka.avro import AvroProducer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from utils.load_avro_schema_from_file import load_avro_schema_from_file
from utils.parse_command_line_args import parse_command_line_args


def send_record(args):
    if not any([args.record_value, args.record_file]):
        raise AttributeError("--record-value or --record-file are not provided.")

    if args.schema_file is None:
        raise AttributeError("--schema-file is not provided.")

    if args.security_protocol and args.security_protocol.lower() not in ['plaintext', 'ssl']:
        raise AttributeError("--security-protocol must be either plaintext or ssl.")

    schema_registry_client = SchemaRegistryClient({
        'url': args.schema_registry
    })

    with open(args.schema_file, 'r') as file:
        schema = file.read()

    string_serializer = StringSerializer('utf-8')
    avro_serializer = AvroSerializer(schema, schema_registry_client)

    producer_config = {
        "bootstrap.servers": args.bootstrap_servers,
        'key.serializer': string_serializer,
        'value.serializer': avro_serializer,
    }

    security_protocol = args.security_protocol.lower()

    if security_protocol == "ssl" and all ([
            args.ssl_ca_location,
            args.ssl_cert_location,
            args.ssl_key_location
        ]):
        producer_config.update({
            'security.protocol': security_protocol,
            'ssl.ca.location': args.ssl_ca_location,
            'ssl.key.location': args.ssl_key_location,
            'ssl.certificate.location': args.ssl_cert_location
        })
    else:
        raise AttributeError("--security-protocol is ssl, please supply certificates.")


    producer = SerializingProducer(producer_config)

    key = args.record_key if args.record_key else str(uuid.uuid4())

    if args.record_file:
        with open(args.record_file, 'r') as f:
            data = f.readlines()
        for line in data:
            try:
                producer.produce(topic=args.topic, key=key, value=json.loads(line))
            except Exception as e:
                print(f"Exception while producing record value - {line} to topic - {args.topic}: {e}")
            else:
                print(f"Successfully producing record value - {line} to topic - {args.topic}")
    else:
        value = args.record_value

        try:
            producer.produce(topic=args.topic, key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value} to topic - {args.topic}: {e}")
        else:
            print(f"Successfully producing record value - {value} to topic - {args.topic}")

    producer.flush()


if __name__ == "__main__":
    send_record(parse_command_line_args())
