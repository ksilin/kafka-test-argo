# kafka-test-argo

Automated smoke, perf & load tests for a Kafka deployment. 


## Smoke test


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

### does the test script work? 




## load test

Start with 1000 messages. 




### Basic Single-Instance Test

```sh
./kafka-load-test.sh -c client.properties -t load-test-topic
```

This will run a test with default settings: 5 steps of increasing load with 1 producer, stopping when average latency exceeds 100ms.

### Advanced Test with Multiple Producers

```sh
./kafka-load-test.sh -c client.properties -t load-test-topic -p 4 -s 8 -l 150 -m 2000000
```

This runs a test with 4 producer instances, 8 load steps, a higher latency threshold of 150ms, and up to 2 million messages per producer in the final step.

### Understanding the Results

The script creates a `load_test_results_[timestamp]` directory with:

CSV files recording latency, throughput, and other metrics for each step

### Text reports summarizing the findings

In multi-instance mode, a master report comparing results across different producer counts



## misc

### testing scripts and configs from local machine

```
k port-forward service/kafka -n confluent 9071:9071
```

```sh
export PATH=$PATH:/Users/ksilin/Code/demos/confluent-7.4.0/bin
```

```sh
kafka-topics --bootstrap-server localhost:9071 --list --command-config ./gke-client.config.local.props
```

you might also need to modify your `/etc/hosts` 



### ArgoCD on GKE password

```
LlQHcV54zUsHHjGm
```