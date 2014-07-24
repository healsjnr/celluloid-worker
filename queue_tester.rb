require 'pry'
require 'celluloid/autostart'
require 'march_hare'
require 'json'
require 'openssl'
require 'benchmark'

require_relative 'queue_manager'

module QueueTester

  TEST_START_EXCHANGE = 'celluloid.test.start'
  TEST_START_QUEUE = 'celluloid.test.start.queue'

  TEST_FINISHED_EXCHANGE = 'celluloid.test.finished'
  TEST_FINISHED_QUEUE = 'celluloid.test.finished.queue'

  TEST_ROUTING_KEY = 'celluloid.test.key'
  TEST_RABBIT_POOL_SIZE = 5

  # Publisher that publishes test messages onto the TEST_START_EXCHANGE
  class Publisher
    def initialize
      @conn = MarchHare.connect
      @channel = @conn.create_channel
      @exchange = @channel.topic(QueueTester::TEST_START_EXCHANGE)
    end

    def publish(message_count)
      (1..message_count).each do |i|
        @exchange.publish({:key => 'test.key', :value => "message_#{i}"}.to_json, :routing_key => QueueTester::TEST_ROUTING_KEY)
      end
    end
  end

  # Consumer that consumes test messages from the TEST_FINSIHED_QUEUE. Also confirms that the message was processed correctly.
  class Consumer
    attr_accessor :consumed, :errors

    def initialize
      @consumed = 0
      @errors = 0
      @conn = MarchHare.connect
      @channel = @conn.create_channel
      @exchange = @channel.topic(QueueTester::TEST_FINISHED_EXCHANGE)
      @queue = @channel.queue(QueueTester::TEST_FINISHED_QUEUE).bind(@exchange, :routing_key => QueueTester::TEST_ROUTING_KEY)
      @queue.subscribe(:ack => true) do |metadata, payload|
        work(metadata, payload)
      end
    end

    def work(metadata, payload)
      data = JSON.parse(payload, :symbolize_names => true)
      derived_result = OpenSSL::HMAC.hexdigest('sha256', data[:key], data[:value])
      if derived_result == data[:result]
        @consumed += 1
      else
        @errors += 1
      end
      metadata.ack
    end

    def reset
      @consumed = 0
      @errors = 0
    end
  end

  # Test Runner: Creates a worker of test_class type and pool_size pool.
  # Then publishes num_messages to the TEST_START_EXCHANGE and ensures
  # that they are all published to the TEST_FINISHED_EXCHANGE.
  class Runner
    attr_accessor :worker_pool

    def initialize(test_class, pool_size)
      publish_opts = []
      publish_queue_opts = {
          :topic => QueueTester::TEST_FINISHED_EXCHANGE,
          :queue => QueueTester::TEST_FINISHED_QUEUE,
          :routing_key => QueueTester::TEST_ROUTING_KEY
      }

      consumer_queue_opts = {
          :topic => QueueTester::TEST_START_EXCHANGE,
          :queue => QueueTester::TEST_START_QUEUE,
          :routing_key => QueueTester::TEST_ROUTING_KEY
      }

      case test_class.to_s
        when PublisherWorker.to_s
          publish_opts << publish_queue_opts
        when SharedPublisherWorker.to_s
          publish_opts << publish_queue_opts
          publish_opts << MarchHare.connect.create_channel.topic(publish_queue_opts[:topic])
        when PooledPublisherWorker.to_s
          publish_opts << publish_queue_opts
          publish_opts << QueuePool.create_rabbit_publish_pool(publish_queue_opts)
        else
          puts "Unknown test class: #{test_class.to_s}"
      end

      puts "Consume opts: #{consumer_queue_opts}"
      puts "Publish opts: #{publish_queue_opts}"

      @worker_pool = QueuePool.new(consumer_queue_opts, pool_size, test_class, *publish_opts)
    end

    def run_test(num_messages)
      puts 'Starting Tests...'
      consumer = Consumer.new
      raw_consumer_start = 0
      publisher = Publisher.new
      time = 1
      total_time = Benchmark.realtime do
        publisher.publish(num_messages)
        puts '  Messages published'
        time = Benchmark.realtime do
          raw_consumer_start = consumer.consumed
          while ((consumer.consumed + consumer.errors) < num_messages)
            sleep 0.01
          end
        end
      end
      puts "\nTests done."
      puts "  Successful: #{consumer.consumed}"
      puts "  Errors: #{consumer.errors}"
      puts "  Total Consumed: #{consumer.consumed + consumer.errors}"
      puts "  Total Published: #{num_messages}"
      puts "  Time: #{time * 1000}ms"
      puts "  Total Throughput: #{num_messages / total_time} req/s"
      puts "  Raw Consumer Throughput: #{(num_messages - raw_consumer_start) / time} req/s"
    end
  end
end

if __FILE__ == $0
  unless ARGV.length == 3
    puts "Usage: queue_tester num_messages pool_size PublisherWorker|SharedPublisherWorker|PooledPublisherWorker"
  end

  num_messages = ARGV[0].to_i
  pool_size = ARGV[1].to_i
  worker = Object.const_get(ARGV[2])
  unless pool_size >= 2
    puts "Pool size must be >= 2"
  end

  puts "Publishing #{num_messages} messages with pool_size: #{pool_size}"
  runner = QueueTester::Runner.new(worker, pool_size)
  runner.run_test(num_messages)
end
