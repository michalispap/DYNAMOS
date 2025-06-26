# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Handles WebSocket connection to AMP's broker.
# Forwards events and messages to AdapterCore.
class BrokerConnection
  def initialize(url, token)
    @url          = url
    @token        = token
    @adapter_core = nil
    @socket       = nil
    @driver       = nil
  end

  # Registers AdapterCore for callbacks.
  def register_adapter_core(adapter_core)
    @adapter_core = adapter_core
  end

  # Opens WebSocket to AMP, sets up event handlers.
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

  # Max close reason length (WebSocket protocol).
  REASON_LENGTH = 123
  private_constant :REASON_LENGTH

  # Closes WebSocket. Truncates reason if too long.
  def close(reason: nil, code: 1000)
    return if @socket.nil?

    # Truncate reason if it exceeds protocol limits.
    if reason && reason.bytesize > REASON_LENGTH
      reason = "#{reason[0, REASON_LENGTH - 3]}..."
    end

    @driver.close(reason, code)
  end

  # Sends binary data to AMP.
  def binary(bytes)
    raise 'No connection to websocket (yet). Is the adapter connected to AMP?' if @driver.nil?

    @driver.binary(bytes)
  end

  private

  # Upgrades TCP socket to SSL.
  def upgrade_to_ssl(socket)
    ssl_socket = OpenSSL::SSL::SSLSocket.new(socket)
    ssl_socket.sync_close = true # also close the wrapped socket
    ssl_socket.connect
    ssl_socket
  end

  # Starts WebSocket driver and read loop.
  def start_listening
    @driver.start
    read_and_forward(@driver)
  end

  # Max bytes to read at once.
  READ_SIZE_LIMIT = 1024 * 1024
  private_constant :READ_SIZE_LIMIT

  # Reads from socket and feeds data to WebSocket driver.
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
