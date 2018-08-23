/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'gdelt_events' --partitions 16 --replication-factor 3
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'gdelt_mentions' --partitions 16 --replication-factor 3
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'gdelt_gkg' --partitions 16 --replication-factor 3
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'gdelt_notify' --partitions 16 --replication-factor 3
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'gdelt_discover' --partitions 16 --replication-factor 3
