# start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

#start kafka server
bin/kafka-server-start.sh config/server.properties

# input topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic input-topic-name


# input topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic input-topic-name2

# output topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic output-topic-name


# generate input topic data
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input-topic-name


# generate input topic data2
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input-topic-name2

# output reader
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic-name --from-beginning