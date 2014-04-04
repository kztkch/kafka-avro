import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("DummyLog.avsc").read())

writer = DataFileWriter(open("DummyLog.avro", "w"), DatumWriter(), schema)
writer.append({"id": 100L, "logTime": 20140401L, "muchoStuff": {"test1": "test1value"}})
writer.append({"id": 101L, "logTime": 20140401L, "muchoStuff": {"test2": "test2value"}})
writer.close()

reader = DataFileReader(open("DummyLog.avro", "r"), DatumReader())
for user in reader:
    print user
reader.close()
