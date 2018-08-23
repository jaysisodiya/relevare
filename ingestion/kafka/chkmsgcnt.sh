/usr/local/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic foobar --time -1 --offsets 1 | awk -F ":" '{sum += $3} END {print sum}'
