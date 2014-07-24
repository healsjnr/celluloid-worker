require 'celluloid/autostart'
require 'march_hare'
require 'json'
require 'openssl'

class QueuePool
  #
  # queue_connection : The connection details for the queue to consume from:
  # {
  #   :topic => 'The topic exchange that is being published to.',
  #   :queue => 'The queue to be consumed from.',
  #   :routing_key => 'The routing key to map message from the exchange to the queue'
  # }
  #
  # pool_size : The number of actors to create in the pool
  #
  # worker_type : The Class of the worker the implements the actor
  #
  # *worker_args : An array of the arguments to be passed to the worker when it is initialized
  #
  def initialize(queue_connection, pool_size, worker_type, *worker_args)
    puts "Creating Queue Pool: #{queue_connection}, pool_size: #{pool_size}, worker_type: #{worker_type}"
    @pool = worker_type.pool(size: pool_size, args: worker_args)

    @conn = MarchHare.connect
    @channel = @conn.create_channel
    @exchange = @channel.topic(queue_connection[:topic])
    @queue = @channel.queue(queue_connection[:queue]).bind(@exchange, :routing_key => queue_connection[:routing_key])
    @queue.subscribe(:ack => true) do |metadata, payload|
      @pool.async.work(metadata, payload)
    end
  end

  # Creates a pool of RabbitMQ publishers that can be shared by the actors
  # opts : {:topic => 'The topic exchange to publish to'}
  # pool_size : The number of RabbitMQ publisher actors to create in the pool
  def self.create_rabbit_publish_pool(opts, pool_size = RabbitPublishWorker::DEFAULT_POOL_SIZE)
    RabbitPublishWorker.pool(size: pool_size, args: [opts])
  end
end

class RabbitPublishWorker
  include Celluloid

  DEFAULT_POOL_SIZE = 2
  # Opts: {:topic => 'The topic exchange to publish to'}
  def initialize(opts)
    puts "RabbitPublishWorker create with opts: #{opts}"
    @opts = opts
    @conn = MarchHare.connect
    @channel = @conn.create_channel
    @exchange = @channel.topic(@opts[:topic])
  end

  def publish(data, routing_key)
    @exchange.publish(data, :routing_key => routing_key)
  end
end

class PublisherWorker
  include Celluloid

  finalizer :queue_finalize

  #
  # opts: {:topic => 'The topic exchange to publish to', :routing_key => 'routing key to publish with'}
  #
  def initialize(opts)
    puts "PublisherWorker create with opts: #{opts}"
    @opts = opts
    @conn = MarchHare.connect
    @channel = @conn.create_channel
    @exchange = @channel.topic(@opts[:topic])
  end

  def work(metadata, payload)
    data = JSON.parse(payload, :symbolize_names => true)
    data[:result] = OpenSSL::HMAC.hexdigest('sha256', data[:key], data[:value])
    @exchange.publish(data.to_json, :routing_key => @opts[:routing_key])
    metadata.ack
    print '.'
  end

  def queue_finalize
    @conn.close
  end
end

class SharedPublisherWorker
  include Celluloid

  #
  # opts: {:routing_key => 'routing key to publish with'}
  # publisher: The RabbitMQ publishing to use for publishing.
  #
  def initialize(opts, publisher)
    puts "SharedPublisherWorker create with opts: #{opts}"
    @opts = opts
    @publisher = publisher
  end

  def work(metadata, payload)
    data = JSON.parse(payload, :symbolize_names => true)
    data[:result] = OpenSSL::HMAC.hexdigest('sha256', data[:key], data[:value])
    @publisher.publish(data.to_json, :routing_key => @opts[:routing_key])
    metadata.ack
    print '.'
  end
end

class PooledPublisherWorker
  include Celluloid

  #
  # opts: {:routing_key => 'routing key to publish with'}
  # pool: The RabbitMQ publishing worker pool to use for publishing.
  #
  def initialize(opts, pool)
    puts "PooledPublisherWorker create with opts: #{opts}"
    @pool = pool
    @opts = opts
  end

  def work(metadata, payload)
    data = JSON.parse(payload, :symbolize_names => true)
    data[:result] = OpenSSL::HMAC.hexdigest('sha256', data[:key], data[:value])
    @pool.async.publish(data.to_json, @opts[:routing_key])
    metadata.ack
    print '.'
  end
end
