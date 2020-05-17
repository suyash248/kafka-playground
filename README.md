# kafka-playground: Producer and multithreaded consumer


![alt text](https://github.com/suyash248/kafka-playground/blob/master/src/main/resources/multithreaded_consumer_trasnparent.png "Overview")


#### Notes -

- Each worker thread will have it’s own queue to store tasks/messages.
- One partition will be handled by only one thread, hence in-order processing can be done and commits can be simplified.
- ATLEAST_ONCE & ATMOST_ONCE delivery semantics can be supported.
- Once a consumer is added/removed, along with partitions, worker threads will also be re-balanced.
- On each consumer, number of worker threads can’t be greater than number of partitions in subscribed topic.
