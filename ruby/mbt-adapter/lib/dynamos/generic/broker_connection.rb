# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Manages the WebSocket connection with AMP's broker.
# Calls back on AdapterCore for message handling and lifecycle events.
class BrokerConnection
  def initialize(url, token)
    @url          = url
    @token        = token
    @adapter_core = nil
    @socket       = nil
    @driver       = nil
  end

  def register_adapter_core(adapter_core)
    @adapter_core = adapter_core
  end

  # Establishes WebSocket connection to AMP and sets up event handlers.
  def connect
    uri = URI.parse(@url)
    @socket = TCPSocket.new(uri.host, uri.port)
    @socket = upgrade_to_ssl(@socket)
    @socket.url = @url

    @driver = WebSocket::Driver.client(@socket)
    @driver.set_header('Authorization', "Bearer #{@token}")

    @driver.on :open do
      logger.info 'Connected to AMP.'
      logger.info "URL: #{@url}"
      @adapter_core.on_open # Notify core that connection is open.
    end

    @driver.on :close do |event|
      logger.info 'Disconnected from AMP.'
      @adapter_core.on_close(event.code, event.reason) # Notify core about closure.
    end

    @driver.on :message do |event|
      @adapter_core.handle_message(event.data.bytes) # Forward received messages to core.
    end

    start_listening
  end

  # Maximum length of a close reason in bytes (WebSocket protocol limit).
  REASON_LENGTH = 123
  private_constant :REASON_LENGTH

  # Closes the WebSocket connection.
  # @param [Integer] code The close code.
  # @param [String] reason The reason for closing.
  def close(reason: nil, code: 1000)
    return if @socket.nil?

    # Truncate reason if it exceeds protocol limits.
    if reason && reason.bytesize > REASON_LENGTH
      reason = "#{reason[0, REASON_LENGTH - 3]}..."
    end

    @driver.close(reason, code)
  end

  # Sends binary data over the WebSocket.
  def binary(bytes)
    raise 'No connection to websocket (yet). Is the adapter connected to AMP?' if @driver.nil?

    @driver.binary(bytes)
  end

  private

  # Upgrades a standard TCPSocket to an SSLSocket for secure communication.
  def upgrade_to_ssl(socket)
    ssl_socket = OpenSSL::SSL::SSLSocket.new(socket)
    ssl_socket.sync_close = true # also close the wrapped socket
    ssl_socket.connect
    ssl_socket
  end

  # Initiates the WebSocket driver and starts the read loop.
  def start_listening
    @driver.start
    read_and_forward(@driver)
  end

  # Max bytes to read from socket in one operation.
  READ_SIZE_LIMIT = 1024 * 1024
  private_constant :READ_SIZE_LIMIT

  # Continuously reads data from the socket and forwards it to the WebSocket driver.
  def read_and_forward(connector)
    loop do
      begin
        break if @socket.eof? # Exit loop if socket is closed.

        data = @socket.read_nonblock(READ_SIZE_LIMIT)
      rescue IO::WaitReadable
        @socket.wait_readable # Wait until socket is readable.
        retry
      rescue IO::WaitWritable
        @socket.wait_writable # Wait until socket is writable.
        retry
      end

      # The driver's parse method emits :open, :close, and :message events.
      connector.parse(data)
    end
  end
end
