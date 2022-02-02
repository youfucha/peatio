require 'kafka'

module Stream
  class <<self
    def connection
      @connection ||= ::Kafka.new(ENV.fetch("KAFKA_URL", "localhost:9092").split(','))
    end

    def producer
      @producer ||= connection.producer
    end

    def consumer
      @consumer ||= connection.consumer
    end

    def produce(topic, payload={})
      payload = JSON.dump payload

      producer.produce(payload, topic: topic)
      producer.deliver_messages
    end

    def enqueue_event(kind, id, event, payload)
      payload = JSON.dump payload

      producer.produce(payload, topic: "rango:events", partition_key: [kind, id, event].join("."))
      producer.deliver_messages
    end

  end
end
