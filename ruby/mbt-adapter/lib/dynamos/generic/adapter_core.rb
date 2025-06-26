# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# AdapterCore: main state machine for the adapter.
# Talks to AMP (via BrokerConnection) and SUT (via Handler).
# Handles protobuf encoding/decoding and message routing.
class AdapterCore
  # Adapter states.
  module State
    DISCONNECTED  = :disconnected
    CONNECTED     = :connected
    ANNOUNCED     = :announced
    CONFIGURED    = :configured
    READY         = :ready
    ERROR         = :error
  end

  # Sets up core, queues, and state.
  def initialize(name, broker_connection, handler)
    @name = name
    @broker_connection = broker_connection
    @handler = handler
    @state = State::DISCONNECTED

    @qthread_to_amp =
      QThread.new { |item| send_message_to_amp(item) }
    @qthread_handle_message =
      QThread.new { |item| parse_and_handle_message(item) }
  end

  # Starts the adapter: connects to AMP.
  def start
    clear_qthread_queues

    case @state
    when State::DISCONNECTED
      logger.info "Connecting to AMP's broker."
      @broker_connection.connect
    else
      message = 'Adapter started while already connected.'
      logger.info(message)
      send_error(message)
    end
  end

  # Called when WebSocket opens. Announces to AMP.
  def on_open
    logger.info 'on_open'

    case @state
    when State::DISCONNECTED
      @state = State::CONNECTED
      logger.info 'Sending announcement to AMP.'

      labels = @handler.supported_labels
      configuration = @handler.configuration
      send_announcement(@name, labels, configuration)
      @state = State::ANNOUNCED
    else
      message = 'Connection opened while already connected.'
      logger.info(message)
      send_error(message)
    end
  end

  # Called when WebSocket closes. Stops handler and reconnects.
  def on_close(code, reason)
    @state = State::DISCONNECTED
    message = "Connection closed with code #{code}, and reason: #{reason}."
    message += ' The server may not be reachable.' if code == 1006

    logger.info(message)
    @handler.stop
    logger.info 'Reconnecting to AMP.'
    start # reconnect to AMP - keep the adapter alive
  end

  # Handles config from AMP: configures handler and starts it.
  def on_configuration(configuration)
    logger.info 'on_configuration'

    case @state
    when State::ANNOUNCED
      logger.info 'Test run is started.'
      logger.info 'Registered configuration.'
      @handler.configuration = configuration
      @state = State::CONFIGURED
      @handler.start
      # The handler should call send_ready as it knows when it is ready.
    when State::CONNECTED
      message = 'Configuration received from AMP while not yet announced.'
      logger.info(message)
      send_error(message)
    else
      message = 'Configuration received while already configured.'
      logger.info(message)
      send_error(message)
    end
  end

  # Handles label (stimulus) from AMP: passes to handler.
  def on_label(label)
    logger.info "on_label: #{label.label}"

    case @state
    when State::READY
      # We do not check that the label is indeed a stimulus
      logger.info 'Forwarding label to Handler object.'
      @handler.stimulate(label)
      # physical_label = @handler.stimulate(label)
      # send_stimulus(label, physical_label, Time.now, label.correlation_id)
    else
      message = 'Label received from AMP while not ready.'
      logger.info(message)
      send_error(message)
    end
  end

  # Handles reset from AMP: resets handler.
  def on_reset
    case @state
    when State::READY
      @handler.reset
      # The handler should call send_ready as it knows when it is ready.
    else
      message = 'Reset received from AMP while not ready.'
      logger.info(message)
      send_error(message)
    end
  end

  # Handles error from AMP: closes connection.
  def on_error(message)
    @state = State::ERROR
    logger.info "Error message received from AMP: #{message}"
    @broker_connection.close(reason: message, code: 1000) # 1000 is normal closure
  end

  # Queues AMP message for handling.
  def handle_message(data)
    logger.debug 'Adding message from AMP to the queue to be handled'
    @qthread_handle_message << data
  end

  # Sends response label to AMP.
  # physical_label: what SUT returned (string/JSON)
  # timestamp: when SUT responded
  def send_response(label, physical_label, timestamp)
    logger.info "Sending response to AMP: #{label.label}."
    label = label.dup
    label.physical_label = physical_label if physical_label
    label.timestamp = time_to_nsec(timestamp)
    queue_message_to_amp(PluginAdapter::Api::Message.new(label: label))
  end

  # Tells AMP we're ready.
  def send_ready
    logger.info "Sending 'Ready' to AMP."
    ready = PluginAdapter::Api::Message::Ready.new
    queue_message_to_amp(PluginAdapter::Api::Message.new(ready: ready))
    @state = State::READY
  end

  # Sends error to AMP and closes connection.
  def send_error(message)
    logger.info "Sending 'Error' to AMP and closing the connection."
    error = PluginAdapter::Api::Message::Error.new(message: message)
    queue_message_to_amp(PluginAdapter::Api::Message.new(error: error))
    @broker_connection.close(reason: message, code: 1000) # 1000 is normal closure
  end

  # Announces supported labels/config to AMP.
  def send_announcement(name, labels, configuration)
    announcement = PluginAdapter::Api::Announcement.new(
      name: name,
      labels: labels,
      configuration: configuration
    )
    queue_message_to_amp(PluginAdapter::Api::Message.new(announcement: announcement))
  end

  # Confirms stimulus to AMP (echoes what we sent to SUT).
  def send_stimulus_confirmation(label, physical_label, timestamp)
    logger.info "Sending stimulus (back) to AMP: #{label.label}."
    label = label.dup
    label.physical_label = physical_label if physical_label
    label.timestamp = time_to_nsec(timestamp)
    queue_message_to_amp(PluginAdapter::Api::Message.new(label: label))
  end

  private

  # Decodes AMP message and routes to handler.
  def parse_and_handle_message(data)
    logger.info 'handle_message'

    payload = data.pack('c*')
    message = PluginAdapter::Api::Message.decode(payload)

    case message.type
    when :configuration
      logger.info 'Received configuration from AMP.'
      on_configuration(message.configuration)

    when :label
      logger.info "Received label from AMP: #{message.label.label}."
      on_label(message.label)

    when :reset
      logger.info "'Reset' received from AMP."
      on_reset

    when :error
      on_error(message.error.message)

    else
      message = "Received message with type #{message.type} which "\
                'is *not* supported.'
      logger.error(message)
    end
  end

  # Empties both internal queues.
  def clear_qthread_queues
    logger.debug 'Clearing queues with pending messages'
    @qthread_to_amp.clear_queue
    @qthread_handle_message.clear_queue
  end

  # Queues message to send to AMP.
  def queue_message_to_amp(message)
    logger.debug 'Adding message to the queue to AMP'
    @qthread_to_amp << message
  end

  # Actually sends protobuf message to AMP.
  def send_message_to_amp(message)
    logger.debug 'Sending message AMP'
    @broker_connection.binary(message.to_proto.bytes)
  end

  # Nanoseconds in a second.
  NSEC_PER_SEC = 1_000_000_000
  private_constant :NSEC_PER_SEC

  # Microseconds in a nanosecond.
  USEC_PER_NSEC = 1_000
  private_constant :USEC_PER_NSEC

  # Converts Time to nanoseconds since epoch.
  def time_to_nsec(time)
    return 0 if time.nil?

    seconds = time.to_i
    nanoseconds = time.nsec
    (seconds * NSEC_PER_SEC) + nanoseconds
  end
end
