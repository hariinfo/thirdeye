
### Create topics
Use this step to create a non compacted kafka topic
~/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic thirdeye_cleansed

Use this step to create a compacted kafka topic
~/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic airline_raw --replication-factor 1 --partitions 1 --config "cleanup.policy=compact" --config "delete.retention.ms=100"  --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01"

### List existing topics
~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --list

### Purge topic
~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic airline_raw --config retention.ms=1000
wait for a min...
~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic airline_raw --delete-config retention.ms

### Delete topic
~/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic airline_raw

### Count of messages in a topic
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic airline_raw --time -1 --offsets 1 | awk -F ":" '{sum += $3} END {print sum}'

### Consume messages from a topic
~/kafka/bin/kafka-console-consumer.sh --new-consumer --bootstrap-server localhost:9092 --topic airline_raw --from-beginning

### Earliest message
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic airline_raw --time -2

### Latest message offset
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic airline_raw --time -1

### Get the earliest offset still in a topic
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic airline_raw --time -2

### Get the consumer offsets for a topic
~/kafka/bin/kafka-consumer-offset-checker.sh --zookeeper=localhost:2181 --topic=airline_raw --group=es-group

