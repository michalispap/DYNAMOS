class RabbitMQService
  @@messages = []

  def initialize(amp_handler, queue_name: 'mbt_testing_queue')
    @amq_user = ENV['AMQ_USER']
    @amq_password = ENV['AMQ_PASSWORD']
    @rabbit_port = '5672'
    @rabbit_dns = 'rabbitmq.core.svc.cluster.local'
    @queue_name = queue_name
    @connection = nil
    @channel = nil
    @queue = nil
    @amp_handler = amp_handler # Reference to an AMP handler for sending errors

    #connect
    #start_consuming
  end

  def on_connected(&block)
    @on_connected = block
  end

  def connect
    @connection = Bunny.new(host: @rabbit_dns, port: @rabbit_port, username: @amq_user,
                            password: @amq_password)
    @connection.start
    @channel = @connection.create_channel
    @queue = @channel.queue(@queue_name, durable: true)
    logger.debug "Queue '#{@queue_name}' is ready."
    @on_connected&.call
    start_consuming
  rescue Bunny::TCPConnectionFailedForAllHosts => e
    message = "Connection failed: #{e.message}"
    logger.error(message)
    # @amp_handler&.send_error_to_amp(message)
    raise
  rescue StandardError => e
    message = "An unexpected error occurred: #{e.message}"
    logger.error(message)
    # @amp_handler&.send_error_to_amp(message)
    raise
  end

  def start_consuming
    logger.info "Waiting for messages on '#{@queue_name}'..."
    @queue.subscribe(block: false) do |_delivery_info, properties, body|
      logger.info "Received properties #{properties}"
      logger.info "Received message: #{body}"

      # parse_message now returns { type: string, payload: ruby_hash, raw_json: string } or original body if not JSON/Proto
      parsed_data = parse_message(body)

      if parsed_data.is_a?(Hash) && parsed_data[:payload]
        store_message(parsed_data[:payload]) # Store the Ruby hash payload
        @amp_handler&.process_rabbitmq_message(parsed_data)
      elsif parsed_data # It was a non-JSON message, or proto decoding failed but we have raw_json
        store_message(parsed_data.is_a?(Hash) ? parsed_data[:raw_json] : parsed_data) # Store raw
        # Decide if you want to send non-successfully decoded messages to AMP or just log
        logger.warn "RabbitMQService: Message was not fully processed into a payload, not sending to AMP handler: #{parsed_data.inspect}"
      end
    end
  rescue Interrupt
    close
    logger.info 'Consumer interrupted. Connection closed.'
  end

  def send_message(message)
    @channel.default_exchange.publish(message, routing_key: @queue.name)
    logger.info("Sent message to queue '#{@queue_name}': #{message}")
  end

  # Close the connection gracefully
  def close
    if @connection&.open?
      @connection.close
      logger.info('Connection closed.')
    else
      message = 'Connection was already closed.'
      logger.warn(message)
      @amp_handler&.send_error_to_amp(message)
    end
  rescue StandardError => e
    message = "Error while closing connection: #{e.message}"
    logger.error(message)
    @amp_handler&.send_error_to_amp(message)
  end

  def parse_message(json_message)
    begin
      parsed_envelope = JSON.parse(json_message)
      type = parsed_envelope['Type']
      base64_body = parsed_envelope['Body']

      unless type && base64_body
        logger.error "RabbitMQ message is missing 'Type' or 'Body': #{json_message}"
        return { type: nil, payload: nil, raw_json: json_message }
      end

      proto_binary = Base64.decode64(base64_body)
      klass = klass_from_type(type)

      if klass
        decoded_proto_obj = klass.decode(proto_binary)
        ruby_payload = dynamos_object_to_hash(decoded_proto_obj)
        logger.info "Successfully decoded RabbitMQ message type '#{type}' into hash."
        logger.debug "Decoded payload for '#{type}': #{ruby_payload.inspect}"
        return { type: type, payload: ruby_payload, raw_json: json_message }
      else
        logger.error("Unknown message type: #{type}. Cannot decode Protobuf.")
        # Return raw JSON if Protobuf class is unknown, so it can still be logged or handled
        return { type: type, payload: nil, raw_json: json_message }
      end
    rescue JSON::ParserError
      logger.warn("Received non-JSON message: #{json_message.inspect}")
      json_message # Return the raw message
    rescue StandardError => e
      logger.error("Error processing RabbitMQ message type '#{type if defined?(type)}': #{e.message} - #{e.backtrace.first}")
      # Return raw JSON in case of other errors during parsing/decoding
      return { type: (type if defined?(type)), payload: nil, raw_json: json_message }
    end
  end

  # Helper method to store messages in memory
  def store_message(message)
    logger.info("Received and stored message: #{message}")
    @@messages << message
  end

  # Method to retrieve all stored messages
  def self.get_stored_messages
    @@messages
  end

  def klass_from_type(type)
    # Converts "someMessageType" to "SomeMessageType" or "sqlDataRequest" to "SqlDataRequest"
    class_name = type.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2') # Handle sequences like SQLData
                     .gsub(/([a-z\d])([A-Z])/, '\1_\2')    # Handle camelCase like dataProviders
                     .split('_')
                     .map(&:capitalize)
                     .join
    if Dynamos.const_defined?(class_name, false) # false to not trigger autoload
      Dynamos.const_get(class_name, false)
    else
      logger.warn "Protobuf class Dynamos::#{class_name} not found for type '#{type}'."
      nil
    end
  rescue NameError
    logger.warn "Error resolving Protobuf class Dynamos::#{class_name} for type '#{type}' (NameError)."
    nil
  end

  private

  def dynamos_object_to_hash(obj)
    case obj
    when Google::Protobuf::Any
      # Handle Google::Protobuf::Any specifically.
      return {
        'type_url' => obj.type_url,
        # Ensure the value is Base64 encoded string
        'value_base64' => Base64.strict_encode64(obj.value)
      }
    when Google::Protobuf::MessageExts
      # This handles all other Protobuf message types.
      result = {}
      obj.class.descriptor.each do |field_descriptor|
        field_name = field_descriptor.name
        value = obj.send(field_name.to_sym) # Get the field's value
        result[field_name] = dynamos_object_to_hash(value) # Recursively convert the value
      end
      result
    when Google::Protobuf::RepeatedField
      # Convert RepeatedField to a Ruby Array, recursively converting its elements.
      obj.map { |item| dynamos_object_to_hash(item) }
    when Google::Protobuf::Map
      # Convert Map to a Ruby Hash, recursively converting its values.
      obj.to_h.transform_values { |v_item| dynamos_object_to_hash(v_item) }
    when String, Numeric, TrueClass, FalseClass
      # Return primitive types as is.
      obj
    when NilClass
      # Return nil as is.
      nil
    else
      # Fallback for any other types (should ideally not be reached with well-defined protos).
      logger.warn "dynamos_object_to_hash: Unhandled type #{obj.class.name}, attempting to_s."
      obj.to_s
    end
  end
end
