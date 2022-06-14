mvn clean package

docker cp \
  target/sn-pulsar-flink-workshop-0.1.0.jar \
  sn-pulsar-flink-workshop-taskmanager-1:opt/flink/job.jar

docker exec -it sn-pulsar-flink-workshop-taskmanager-1 \
  ./bin/flink run --class io.ipolyzos.compute.fault_tolerance.EnrichmentStream \
  job.jar