celluloid-worker
================

Spike solution for using celluloid + JRuby + MarchHare

##queue_manager.rb:

  All workers perform the following:
  
  * Create the TEST_START_QUEUE and bind it to the TEST_START_EXCHANGE using routing key TEST_ROUTING_KEY
  * For each message:
    * Retrieve the message of the queue.
    * Parse the JSON
    * Create a HMAC of the :key and :value in message
    * Publish {:key, :value, :result => HMAC} back to the TEST_FINISHED_EXCHANGE

###QueuePool: 
A pool manager for the queue workers. This creates a pool of actors given the actor type and pool size. It will also connect to the consumer queue specified.
  
###PublisherWorker: 
A queue worker that creates it's own RabbitMQ publisher. That is, every actor will have it's connection to RabbitMQ to publish messages on. This could be a problem if we have a large number of workers (ie., > 50).
  
###SharedPublisherWorker: 
All workers used a single shared publisher. I was expecting this worker to crash under heavy load due to multi-threading issues, however, it worked without a problem.
  
###PooledPublisherWorker: 
A worker that uses a pool of RabbitMQPublishers to publish. The pool is created separately and passed in. Messages are published by calling pool.async.publish. This allow us to have a small number of RabbitMQ publishers (ie., 2) for a large number of actors (ie., 100).
  
  
##queue_tester.rb:

  Test framework for the above pool and worker clases:
  
  * Creates Pool and Worker
  * Creates a publisher that publishes the request number of messages to the TEST_START_EXCHANGE
  * Creates a consumer that consumes messages from the TEST_FINISHED_QUEUE and verifies that they are correct (ie., recalculate the HMAC).
  
###Running the test harness:
  
    $ bundle install
    $ bundle exec ruby queue_tester.rb num_messages num_actors PublisherWorker|SharedPublisherWorker|PooledPublisherWorker
  

