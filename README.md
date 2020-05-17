# kafka-playground: Producer and multithreaded consumer


![alt text](https://github.com/suyash248/kafka-playground/blob/master/src/main/resources/multithreaded_consumer_trasnparent.png "Overview")


#### Notes -

- Each worker thread will have it’s own queue to store tasks/messages.
- One partition will be handled by only one thread, hence in-order processing can be done and commits can be simplified.
- If more such consumers are added, worker threads can be re-balanced.
- ATLEAST_ONCE & ATMOST_ONCE delivery semantics can be supported.
- Refer next slide for visuals.
- On each consumer, number of worker threads can’t be greater than number of partitions in subscribed topic.
