# Handles RabbitMQ connection, message consumption, and Protobuf processing.
# RabbitMQ messages are expected as Protobuf payloads in a JSON envelope.
class RabbitMQService
  # In-memory store for received messages (for debugging).
  @@messages = []

  # @param amp_handler [DynamosHandler] Main adapter handler.
  # @param queue_name [String] RabbitMQ queue name.
  def initialize(amp_handler, queue_name: 'mbt_testing_queue')
    @amq_user = ENV['AMQ_USER']
    @amq_password = ENV['AMQ_PASSWORD']
    @rabbit_port = '5672'
    @rabbit_dns = 'rabbitmq.core.svc.cluster.local'
    @queue_name = queue_name
    @connection = nil # Bunny RabbitMQ connection.
    @channel = nil    # Bunny RabbitMQ channel.
    @queue = nil      # Bunny RabbitMQ queue.
    @amp_handler = amp_handler # To pass messages to AMP.
  end

  # Registers a callback for when RabbitMQ connection is established.
  def on_connected(&block)
    @on_connected = block
  end

  # Connects to RabbitMQ, creates channel, and declares queue.
  def connect
    @connection = Bunny.new(host: @rabbit_dns, port: @rabbit_port, username: @amq_user,
                            password: @amq_password)
    @connection.start
    @channel = @connection.create_channel
    # 'durable: true' = queue survives broker restarts.
    @queue = @channel.queue(@queue_name, durable: true)
    logger.debug "Queue '#{@queue_name}' is ready."
    @on_connected&.call # Execute callback.
    start_consuming     # Start listening.
  rescue Bunny::TCPConnectionFailedForAllHosts => e
    logger.error("RabbitMQ connection failed: #{e.message}")
    raise # Halt if connection is critical.
  rescue StandardError => e
    logger.error("Unexpected error during RabbitMQ connect: #{e.message}")
    raise
  end

  # Subscribes to RabbitMQ queue and processes messages asynchronously.
  def start_consuming
    logger.info "Waiting for messages on RabbitMQ queue '#{@queue_name}'..."
    # 'block: false' = non-blocking subscription.
    @queue.subscribe(block: false) do |_delivery_info, properties, body|
      # body is expected to be a JSON string (message envelope).
      logger.info "Received RabbitMQ properties #{properties}"
      logger.info "Received RabbitMQ message: #{body}"

      parsed_data = parse_message(body) # Handles JSON & Protobuf decoding.

      if parsed_data.is_a?(Hash) && parsed_data[:payload]
        store_message(parsed_data[:payload]) # Store decoded payload.
        # Pass {type, payload, raw_json} to AMP handler.
        @amp_handler&.process_rabbitmq_message(parsed_data)
      elsif parsed_data # If parsing failed but raw data exists.
        store_message(parsed_data.is_a?(Hash) ? parsed_data[:raw_json] : parsed_data)
        logger.warn "RabbitMQService: Message not fully processed, not sending to AMP: #{parsed_data.inspect}"
      end
    end
  rescue Interrupt # Handle Ctrl+C.
    close
    logger.info 'RabbitMQ consumer interrupted. Connection closed.'
  end

  # Publishes a message to the configured RabbitMQ queue.
  def send_message(message)
    # Publishes to default exchange, routing to queue by its name.
    @channel.default_exchange.publish(message, routing_key: @queue.name)
    logger.info("Sent message to RabbitMQ queue '#{@queue_name}': #{message}")
  end

  # Closes the RabbitMQ connection.
  def close
    if @connection&.open?
      @connection.close
      logger.info('RabbitMQ connection closed.')
    else
      logger.warn('RabbitMQ connection was already closed.')
    end
  rescue StandardError => e # Handle errors during close.
    logger.error("Error while closing RabbitMQ connection: #{e.message}")
    @amp_handler&.send_error_to_amp("Error closing RabbitMQ: #{e.message}")
  end

  # Parses JSON envelope from RabbitMQ, decodes Base64 Protobuf body.
  # @param json_message [String] Raw JSON message string.
  # @return [Hash, String] { type, payload (Hash), raw_json } or original message on error.
  def parse_message(json_message)
    begin
      parsed_envelope = JSON.parse(json_message)
      type = parsed_envelope['Type']         # Protobuf message type.
      base64_body = parsed_envelope['Body'] # Base64 encoded Protobuf.

      unless type && base64_body # Ensure essential fields.
        logger.error "RabbitMQ message missing 'Type' or 'Body': #{json_message}"
        return { type: nil, payload: nil, raw_json: json_message }
      end

      proto_binary = Base64.decode64(base64_body)
      klass = klass_from_type(type) # Get Ruby class for Protobuf type.

      if klass
        decoded_proto_obj = klass.decode(proto_binary) # Decode binary Protobuf.
        ruby_payload = dynamos_object_to_hash(decoded_proto_obj) # Convert Protobuf to Ruby hash.
        logger.info "Successfully decoded RabbitMQ message type '#{type}'."
        logger.debug "Decoded payload for '#{type}': #{ruby_payload.inspect}"
        return { type: type, payload: ruby_payload, raw_json: json_message }
      else
        logger.error("Unknown message type: #{type}. Cannot decode Protobuf.")
        return { type: type, payload: nil, raw_json: json_message } # Return with type if class unknown.
      end
    rescue JSON::ParserError # Invalid JSON envelope.
      logger.warn("Received non-JSON message from RabbitMQ: #{json_message.inspect}")
      json_message # Return raw message.
    rescue StandardError => e # Other parsing/decoding errors.
      logger.error("Error processing RabbitMQ message type '#{type if defined?(type)}': #{e.message} - #{e.backtrace.first}")
      return { type: (type if defined?(type)), payload: nil, raw_json: json_message } # Return with type if error.
    end
  end

  # Stores decoded message payload in memory.
  def store_message(message)
    logger.info("Received and stored message: #{message}")
    @@messages << message
  end

  # Retrieves all stored messages.
  def self.get_stored_messages
    @@messages
  end

  # Converts message type string (e.g., "sqlDataRequest") to Ruby Protobuf class.
  # @param type [String] Message type from JSON envelope.
  # @return [Class, nil] Ruby Protobuf class or nil if not found.
  def klass_from_type(type)
    # Converts type string (e.g., "requestApproval") to Ruby class name ("RequestApproval").
    # Handles initialisms like "SQLDataRequest" -> "SqlDataRequest".
    class_name = type.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
                     .gsub(/([a-z\d])([A-Z])/, '\1_\2')
                     .split('_')
                     .map(&:capitalize)
                     .join
    # Check if class is defined under Dynamos module. 'false' = don't search ancestors.
    if Dynamos.const_defined?(class_name, false)
      Dynamos.const_get(class_name, false)
    else
      logger.warn "Protobuf class Dynamos::#{class_name} not found for type '#{type}'."
      nil
    end
  rescue NameError # If const_get fails.
    logger.warn "Error resolving Protobuf class Dynamos::#{class_name} for type '#{type}' (NameError)."
    nil
  end

  private

  # Recursively converts a Ruby Protobuf object to a plain Ruby hash.
  # @param obj [Object] Protobuf object or primitive value.
  # @return [Hash, Array, String, Numeric, Boolean, NilClass] Converted Ruby data.
  def dynamos_object_to_hash(obj)
    case obj
    when Google::Protobuf::Any
      # Handle Google::Protobuf::Any: return type_url and Base64 value.
      return {
        'type_url' => obj.type_url,
        'value_base64' => Base64.strict_encode64(obj.value)
      }
    when Google::Protobuf::MessageExts # General Protobuf messages.
      result = {}
      # Iterate over Protobuf message fields.
      obj.class.descriptor.each do |field_descriptor|
        field_name = field_descriptor.name
        value = obj.send(field_name.to_sym)
        result[field_name] = dynamos_object_to_hash(value) # Recurse for field value.
      end
      result
    when Google::Protobuf::RepeatedField # Protobuf array.
      obj.map { |item| dynamos_object_to_hash(item) } # Recurse for each element.
    when Google::Protobuf::Map # Protobuf map/dictionary.
      obj.to_h.transform_values { |v_item| dynamos_object_to_hash(v_item) } # Recurse for values.
    when String, Numeric, TrueClass, FalseClass # Primitive types.
      obj
    when NilClass
      nil
    else # Fallback for unexpected types.
      logger.warn "dynamos_object_to_hash: Unhandled type #{obj.class.name}, attempting to_s."
      obj.to_s
    end
  end
end

