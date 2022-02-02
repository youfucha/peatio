# encoding: UTF-8
# frozen_string_literal: true

require File.join(ENV.fetch('RAILS_ROOT'), 'config', 'environment')

raise "bindings must be provided." if ARGV.size == 0

logger = Rails.logger

consumer = ::Stream.consumer

at_exit { consumer.close }

def get_worker(id)
  ::Workers::Engines.const_get(id.to_s.camelize).new
end

workers = []
ARGV.each do |id|
  worker = get_worker(id)

  consumer.subscribe(id)

  loop do
    message = consumer.poll(100)

    next if message.nil?

    logger.info { "Received: #{message.payload}" }

    begin
      payload = JSON.parse(message.payload)

      worker.process(payload)
    rescue StandardError => e
      if worker.is_db_connection_error?(e)
        logger.error(db: :unhealthy, message: e.message)
        exit(1)
      end

      report_exception(e)
    end

    @consumer.commit(nil, true)
  end

  workers << worker
end

%w(USR1 USR2).each do |signal|
  Signal.trap(signal) do
    puts "#{signal} received."
    handler = "on_#{signal.downcase}"
    workers.each {|w| w.send handler if w.respond_to?(handler) }
  end
end

ch.work_pool.join
