# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Adds 'url' attribute to SSLSocket for WebSocket::Driver.
module OpenSSL
  module SSL
    class SSLSocket
      attr_accessor :url
    end
  end
end

# Handles connection to DYNAMOS SUT (via RabbitMQ).
class DynamosConnection
  def initialize(handler)
    @handler = handler
    @queue_handler = nil
  end

  # Connects to RabbitMQ and signals ready.
  def connect
    @queue_handler = RabbitMQService.new(@handler)
    @queue_handler.on_connected { @handler.send_ready_to_amp }
    @queue_handler.connect
  end

  # Closes RabbitMQ connection.
  def close(reason: nil, code: 1000)
    @queue_handler&.close
  end

  # Sends message to SUT via RabbitMQ.
  def send(message)
    @queue_handler.send_message(message)
  end
end
