import avro.schema
import cStringIO
import sys
from avro.io import DatumReader, DatumWriter
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer, KeyedProducer

def kafkahighlevelproducer(kafka_conn, schema, bytes):
    """
    kafka High level API
    """
    print "SimpleProducer start."


    writer =  cStringIO.StringIO()
    encoder = avro.io.BinaryEncoder(writer)
    datum_writer = avro.io.DatumWriter(schema)

    producer = SimpleProducer(kafka_conn)
    for topic in ["DUMMY_LOG"]:
        writer.truncate(0)
        datum_writer.write({"id": 100L, "logTime": 20140401L, "muchoStuff": {"test": "test1value"}}, encoder)
        bytes = writer.getvalue()
        producer.send_messages(topic, bytes)
        writer.truncate(0)
        datum_writer.write({"id": 101L, "logTime": 20140402L, "muchoStuff": {"test": "test2value"}}, encoder)
        bytes = writer.getvalue()
        producer.send_messages(topic, bytes)

    writer.close()

    print "SimpleProducer done."

def kafkahighlevelconsumer(kafka_conn, schema):
    print "SimpleConsumer start."
    for topic in ["DUMMY_LOG"]:
        print "topic=%s\n" % (topic)
        consumer = SimpleConsumer(kafka, "my-group" + topic, topic, auto_commit=False)
        # TODO: how to escape this loop? 
        for message in consumer:

            # Deserialize
            reader = cStringIO.StringIO(message.message.value)
            decoder = avro.io.BinaryDecoder(reader)
            datum_reader = avro.io.DatumReader(schema)
            
            ret = datum_reader.read(decoder)
            print ret["id"]
            print ret["logTime"]
            mucho = ret["muchoStuff"]
            print mucho["test"]
            
            reader.close()

    print "SimpleConsumer done."

###########################################
## kafka Low level API
###########################################

def kafkalowleveltest(kafka, bytes):
    """
    under construction
    """
    print "under construction"

if __name__ == '__main__':
    
    # Schma
    schema = avro.schema.parse(open("DummyLog.avsc").read())

    # Kafka Common
    kafka = KafkaClient("kafka-01:9092")

    kafkahighlevelproducer(kafka, schema, bytes)
    #kafkahighlevelconsumer(kafka, schema)
    

# vim: set nu expandtab tabstop=4 ft=python:
