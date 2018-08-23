echo "Uncomment the commands for deletion"
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic gdelt_events
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic gdelt_mentions
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic gdelt_gkg
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic gdelt_notify
/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic gdelt_discover
