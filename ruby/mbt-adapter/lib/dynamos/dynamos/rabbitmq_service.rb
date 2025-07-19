# Handles RabbitMQ connection, message consumption, and protobuf decoding.
# Expects messages as JSON envelope with base64 protobuf payload.
class RabbitMQService
  @@messages = []

  # Set up connection params.
  def initialize(amp_handler, queue_name: 'mbt_testing_queue')
    @amq_user = ENV['AMQ_USER']
    @amq_password = ENV['AMQ_PASSWORD']
    @rabbit_port = '5672'
    @rabbit_dns = 'rabbitmq.core.svc.cluster.local'
    @queue_name = queue_name
    @amp_handler = amp_handler
  end

  # Register callback for when connected.
  def on_connected(&block)
    @on_connected = block
  end

  # Connects to RabbitMQ and starts consuming.
  def connect
    logger.debug "Attempting to connect as user: #{@amq_user} with password: #{@amq_password} on host #{@rabbit_dns} with port #{@rabbit_port}."
    @connection = Bunny.new(host: @rabbit_dns, port: @rabbit_port, username: @amq_user, password: @amq_password)
    logger.debug "Starting connection..."
    @connection.start
    logger.debug "Creating channel..."
    @channel = @connection.create_channel
    @queue = @channel.queue(@queue_name, durable: true)
    logger.debug "Queue '#{@queue_name}' is ready."
    @on_connected&.call
    logger.debug "Starting consuming..."
    start_consuming
  rescue Bunny::TCPConnectionFailedForAllHosts, StandardError => e
    log_and_raise("RabbitMQ connection failed", e)
  end

  # Starts consuming messages from queue.
  def start_consuming
    logger.info "Waiting for messages on RabbitMQ queue '#{@queue_name}'..."
    @queue.subscribe(block: false) do |_delivery_info, properties, body|
      logger.info "*** Received RabbitMQ properties #{properties}"
      logger.debug "*** Received RabbitMQ message: #{body}"
      handle_incoming_message(body)
    end
  rescue Interrupt
    close
    logger.info 'RabbitMQ consumer interrupted. Connection closed.'
  end

  # Publishes message to RabbitMQ.
  def send_message(message)
    @channel.default_exchange.publish(message, routing_key: @queue.name)
    logger.info("Sent message to RabbitMQ queue '#{@queue_name}': #{message}")
  end

  # Closes RabbitMQ connection.
  def close
    if @connection&.open?
      @connection.close
      logger.info('RabbitMQ connection closed.')
    else
      logger.warn('RabbitMQ connection was already closed.')
    end
  rescue StandardError => e
    log_and_notify_amp("Error while closing RabbitMQ connection", e)
  end

  # Parses JSON message, decodes protobuf, returns hash.
  def parse_message(json_message)
    parsed_envelope = JSON.parse(json_message)
    type = parsed_envelope['type']
    base64_body = parsed_envelope['body']

    unless type && base64_body
      logger.error "RabbitMQ message missing 'type' or 'body': #{json_message}"
      return { type: nil, payload: nil, raw_json: json_message }
    end

    proto_binary = Base64.decode64(base64_body)
    klass = klass_from_type(type)

    if klass
      decoded_proto_obj = klass.decode(proto_binary)
      ruby_payload = dynamos_object_to_hash(decoded_proto_obj)
      logger.info "Successfully decoded RabbitMQ message type '#{type}'."
      logger.debug "Decoded payload for '#{type}': #{ruby_payload.inspect}"
      { type: type, payload: ruby_payload, raw_json: json_message }
    else
      logger.error("Unknown message type: #{type}. Cannot decode Protobuf.")
      { type: type, payload: nil, raw_json: json_message }
    end
  rescue JSON::ParserError
    logger.warn("Received non-JSON message from RabbitMQ: #{json_message.inspect}")
    json_message
  rescue StandardError => e
    logger.error("Error processing RabbitMQ message: #{e.message} - #{e.backtrace.first}")
    { type: (defined?(type) ? type : nil), payload: nil, raw_json: json_message }
  end

  # Stores message in class-level array.
  def store_message(message)
    logger.debug("Received and stored message: #{message}")
    @@messages << message
  end

  # Returns all stored messages.
  def self.get_stored_messages
    @@messages
  end

  # Maps message type to protobuf class.
  def klass_from_type(type)
    class_name = case type
                 when 'anonymizeFinished', 'algorithmFinished', 'aggregateFinished', 'queryFinished'
                   'MicroserviceCommunication'
                 else
                   type.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
                       .gsub(/([a-z\d])([A-Z])/, '\1_\2')
                       .split('_')
                       .map(&:capitalize)
                       .join
                 end
    if Dynamos.const_defined?(class_name, false)
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

  # Handles incoming message: parses, stores, and notifies handler.
  def handle_incoming_message(body)
    parsed_data = parse_message(body)
    if parsed_data.is_a?(Hash) && parsed_data[:payload]
      store_message(parsed_data[:payload])
      @amp_handler&.process_rabbitmq_message(parsed_data)
    elsif parsed_data
      store_message(parsed_data.is_a?(Hash) ? parsed_data[:raw_json] : parsed_data)
      logger.warn "RabbitMQService: Message not fully processed, not sending to AMP: #{parsed_data.inspect}"
    end
  end

  # Logs error and raises.
  def log_and_raise(msg, exception)
    logger.error("#{msg}: #{exception.message}")
    raise
  end

  # Logs error and notifies AMP.
  def log_and_notify_amp(msg, exception)
    logger.error("#{msg}: #{exception.message}")
    @amp_handler&.send_error_to_amp("#{msg}: #{exception.message}")
  end

  # Recursively converts protobuf object to Ruby hash.
  def dynamos_object_to_hash(obj)
    case obj
    when Google::Protobuf::Any
      { 'type_url' => obj.type_url, 'value_base64' => Base64.strict_encode64(obj.value) }
    when Google::Protobuf::MessageExts
      obj.class.descriptor.each_with_object({}) do |field_descriptor, result|
        field_name = field_descriptor.name
        value = obj.send(field_name.to_sym)
        result[field_name] = (field_name == 'data' && !value.nil?) ? "too large" : dynamos_object_to_hash(value)
      end
    when Google::Protobuf::RepeatedField, Array
      obj.map { |item| dynamos_object_to_hash(item) }
    when Google::Protobuf::Map, Hash
      obj.to_h.transform_values { |v| dynamos_object_to_hash(v) }
    when String, Numeric, TrueClass, FalseClass
      obj
    when NilClass
      nil
    else
      logger.warn "dynamos_object_to_hash: Unhandled type #{obj.class.name}, attempting to_s."
      obj.to_s
    end
  end
end
