



### does producer work

```sh
seq 5 | kafka-console-producer --topic demotopic --broker-list kafka.confluent.svc.cluster.local:9071 --producer.config /tmp/kafka.properties
```


### does consumer work? 

```sh
kafka-console-consumer --topic demotopic3 --bootstrap-server kafka.confluent.svc.cluster.local:9071 --consumer.config /tmp/kafka.properties --from-beginning
```


### does producer-pref-test work


```sh
kafka-producer-perf-test --num-records 1000 --record-size 100 --throughput -1 --topic demotopic3 --producer.config /tmp/kafka.properties
```


### ArgoCD on GKE password

```
LlQHcV54zUsHHjGm
```