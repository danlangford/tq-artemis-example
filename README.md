# tq-artemis-example

this is built on top of the Artemis example `clustered-static-discovery` and it replicates what we are seeing in our clustered deployment. Namely that when sending AMQP messages into the cluster we are getting exceptions the moment the server tries to load-balance the message out to another node in the cluster

run `mvn verify` and watch the stock example using Artemis Client (CORE) work then immediatly after see the use of the QPID Client (AMQP) fail sending the second message. 

the `mvn verify` goal will automatically download Artemis 2.1.0 and unzip it into your target directory for isolation and a zeroconfig run on your part. 

The file `StaticClusteredQueueExample.java` and all 4 `broker.xml` files are identical to the [example that ships with artemis](https://github.com/apache/activemq-artemis/tree/2.1.0/examples/features/clustered/clustered-static-discovery). 