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

      store_message(parse_message(body))

      @amp_handler&.send_response_to_amp(message)
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
      parsed = JSON.parse(json_message)
      type = parsed['Type']
      base64_body = parsed['Body']
      proto_binary = Base64.decode64(base64_body)
      klass = klass_from_type(type)
      klass ? klass.decode(proto_binary) : logger.error("Unknown message type: #{type}")
    rescue JSON::ParserError
      logger.warn("Received non-JSON message: #{json_message.inspect}")
      json_message # Return the raw message for further handling
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
    # Capitalize first letter to match the naming convention
    class_name = type[0].upcase + type[1..]
    return unless Dynamos.const_defined?(class_name)

    Dynamos.const_get(class_name)
  end
end

# Example Usage
# if __FILE__ == $0
#   class MockAMPHandler
#     def send_error_to_amp(message)
#       puts "AMP Handler received error: #{message}"
#     end
#   end
#
#   amp_handler = MockAMPHandler.new
#   handler = RabbitMQHandler.new('persistent_queue', amp_handler)
#
#   # Sending messages
#   handler.send_message("Hello, Queue!")
#   handler.send_message("Another message.")
#
#   # Start consuming messages
#   Thread.new { handler.process_messages }
#
#   # Simulate other tasks or check stored messages after some time
#   sleep 5
#   logger.info("Stored messages in memory: #{RabbitMQHandler.get_stored_messages.inspect}")
#
#   # Close the connection gracefully
#   handler.close
# end
#
