# docker run --name test-cassandra-v2 -p 9042:9042 cassandra
# docker exec -it test-cassandra-v2 bash
# cqlsh
# CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
# USE my_keyspace;
# CREATE TABLE campaign_performance (
#   data timestamp,
#   campaign_name text,
#   PRIMARY KEY ((campaign_name), data)
# );
# DESCRIBE TABLE campaign_performance;
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'], port=9042)
session = cluster.connect("my_keyspace")

# print("Reading..")
# rows = session.execute('SELECT * FROM campaign_performance')
# for row in rows:
#     print(row)

# prepared_statement = session.prepare("SELECT * FROM campaign_performance WHERE id=?")

# print("Write sync")
# session.execute("INSERT INTO ...")

# print("Write async")
# session.execute_async("INSERT INTO...")

