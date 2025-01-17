module EventAPI
  class << self
    def notify(event_name, event_payload)
      arguments = [event_name, event_payload]
      middlewares.each do |middleware|
        returned_value = middleware.call(*arguments)
        case returned_value
          when Array then arguments = returned_value
          else return returned_value
        end
      rescue StandardError => e
        report_exception(e)
        raise
      end
    end

    def middlewares=(list)
      @middlewares = list
    end

    def middlewares
      @middlewares ||= []
    end
  end

  module ActiveRecord
    class Mediator
      attr_reader :record

      def initialize(record)
        @record = record
      end

      def notify(partial_event_name, event_payload)
        tokens = ['model']
        tokens << record.class.event_api_settings.fetch(:prefix) { record.class.name.underscore.gsub(/\//, '_') }
        tokens << partial_event_name.to_s
        full_event_name = tokens.join('.')
        EventAPI.notify(full_event_name, event_payload)
      end

      def notify_record_created
        notify(:created, record: record.as_json_for_event_api.compact)
      end

      def notify_record_updated
        return if record.previous_changes.blank?

        current_record  = record
        previous_record = record.dup
        record.previous_changes.each { |attribute, values| previous_record.send("#{attribute}=", values.first) }

        # Guarantee timestamps.
        previous_record.created_at ||= current_record.created_at
        previous_record.updated_at ||= current_record.created_at

        before = previous_record.as_json_for_event_api.compact
        after  = current_record.as_json_for_event_api.compact

        notify :updated, \
          record:  after,
          changes: before.delete_if { |attribute, value| after[attribute] == value }
      end
    end

    module Extension
      extend ActiveSupport::Concern

      included do
        # We add «after_commit» callbacks immediately after inclusion.
        %i[create update].each do |event|
          after_commit on: event, prepend: true do
            if self.class.event_api_settings[:on]&.include?(event)
              event_api.public_send("notify_record_#{event}d")
            end
          end
        end
      end

      module ClassMethods
        def acts_as_eventable(settings = {})
          settings[:on] = %i[create update] unless settings.key?(:on)
          @event_api_settings = event_api_settings.merge(settings)
        end

        def event_api_settings
          @event_api_settings || superclass.instance_variable_get(:@event_api_settings) || {}
        end
      end

      def event_api
        @event_api ||= Mediator.new(self)
      end

      def as_json_for_event_api
        as_json
      end
    end
  end

  # To continue processing by further middlewares return array with event name and payload.
  # To stop processing event return any value which isn't an array.
  module Middlewares

    class << self
      def application_name
        Rails.application.class.name.split('::').first.underscore
      end

      def application_version
        "#{application_name.camelize}::VERSION".constantize
      end
    end

    class IncludeEventMetadata
      def call(event_name, event_payload)
        event_payload[:name] = event_name
        [event_name, event_payload]
      end
    end

    class GenerateJWT
      def call(event_name, event_payload)
        jwt_payload = {
            iss:   Middlewares.application_name,
            jti:   SecureRandom.uuid,
            iat:   Time.now.to_i,
            exp:   Time.now.to_i + 60,
            event: event_payload
        }

        private_key = OpenSSL::PKey.read(Base64.urlsafe_decode64(ENV.fetch('EVENT_API_JWT_PRIVATE_KEY')))
        algorithm   = ENV.fetch('EVENT_API_JWT_ALGORITHM')
        jwt         = JWT::Multisig.generate_jwt jwt_payload, \
                                                   { Middlewares.application_name.to_sym => private_key },
                                                 { Middlewares.application_name.to_sym => algorithm }

        [event_name, jwt]
      end
    end

    class PrintToScreen
      def call(event_name, event_payload)
        Rails.logger.debug do
          ['',
           'Produced new event at ' + Time.current.to_s + ': ',
           'name    = ' + event_name,
           'payload = ' + event_payload.to_json,
           ''].join("\n")
        end
        [event_name, event_payload]
      end
    end

    class PublishToKafka
      extend Memoist

      def call(event_name, event_payload)
        topic = "#{Middlewares.application_name}.events.#{event_name.split(".").first}"

        Rails.logger.debug do
          "\nPublishing to #{topic} with key: #{event_name} (exchange name).\n"
        end

        ::Stream.producer.produce(payload: event_payload.to_json, topic: topic, key: event_name)
      end
    end
  end

  middlewares << Middlewares::IncludeEventMetadata.new
  middlewares << Middlewares::GenerateJWT.new
  middlewares << Middlewares::PrintToScreen.new
  middlewares << Middlewares::PublishToKafka.new
end
