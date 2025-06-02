# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Add 'url' attribute.
# Required by WebSocket::Driver client for SSL (not directly used by this class).
module OpenSSL
  module SSL
    class SSLSocket
      attr_accessor :url
    end
  end
end

# Manages the connection to the DYNAMOS SUT.
class DynamosConnection
  def initialize(handler)
    @handler = handler # The main adapter handler (DynamosHandler).
    @socket  = nil
    @driver  = nil
    @queue_handler = nil # RabbitMQService instance.
  end

  # Establishes SUT connection (RabbitMQ).
  # Initializes RabbitMQService and sets a callback for when connected.
  def connect
    @queue_handler = RabbitMQService.new(@handler)
    # Callback executed once RabbitMQService is connected.
    @queue_handler.on_connected do
      @handler.send_ready_to_amp # Notify AMP that adapter is ready.
    end
    @queue_handler.connect # Initiate RabbitMQ connection.
  end

  # Closes SUT connection (RabbitMQ).
  # 'reason' and 'code' are for WebSocket API compatibility, not used here.
  def close(reason: nil, code: 1000)
    @queue_handler&.close # Delegate to RabbitMQService.
  end

  # Sends a message to the SUT via RabbitMQ.
  def send(message)
    @queue_handler.send_message(message) # Delegate to RabbitMQService.
  end
end

