#docker exec -it pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/transactions
#docker exec -it pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/accounts
#docker exec -it pulsar bin/pulsar-admin topics create-partitioned-topic -p 1 persistent://public/default/customers

docker exec -it pulsar bin/pulsar-admin topics create persistent://public/default/transactions-json
docker exec -it pulsar bin/pulsar-admin topics create persistent://public/default/transactions-avro
docker exec -it pulsar bin/pulsar-admin topics create persistent://public/default/customers
docker exec -it pulsar bin/pulsar-admin topics create persistent://public/default/credits
docker exec -it pulsar bin/pulsar-admin topics create persistent://public/default/debits

docker exec -it pulsar bin/pulsar-admin topics list public/default

docker exec -it pulsar bin/pulsar-admin topics set-retention -s -1 -t -1 persistent://public/default/customers
docker exec -it pulsar bin/pulsar-admin topics get-retention persistent://public/default/customers