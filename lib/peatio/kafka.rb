require 'rdkafka'

module Stream
  class <<self
    def producer
      @producer ||= ::Rdkafka::Config.new({
        :"bootstrap.servers" => ENV.fetch("KAFKA_URL", "localhost:9092")
      }).producer
    end

    def consumer
      @consumer ||= ::Rdkafka::Config.new({
        :"bootstrap.servers" => ENV.fetch("KAFKA_URL", "localhost:9092"),
        :"group.id" => "zsmartex-" + [*'a'..'z', *0..9, *'A'..'Z'].shuffle[0..10].join,
        :"enable.partition.eof" => false
      }).consumer
    end

    def produce(topic, payload={})
      payload = JSON.dump payload

      producer.produce(payload: payload, topic: topic)
    end

    def enqueue_event(kind, id, event, payload)
      payload = JSON.dump payload

      producer.produce(payload: payload, topic: "rango.events", key: [kind, id, event].join("."))
    end

  end
end
