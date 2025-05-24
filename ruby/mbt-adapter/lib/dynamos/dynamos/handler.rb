# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

class DynamosHandler < Handler
  def initialize
    @connection = nil
    super
  end

  STIMULI = %w[sql_data_request].freeze
  RESPONSES = %w[amqp_receive].freeze
  private_constant :STIMULI, :RESPONSES

  DYNAMOS_URL = 'ws://127.0.0.1:3001'

  # Prepare to start testing.
  def start
    return unless @connection.nil?

    logger.info 'Starting. Trying to connect to the SUT.'
    @connection = DynamosConnection.new(self)
    @connection.connect
    # When the connection is open, the :open callback will send Ready to AMP.
  end

  # Stop testing.
  def stop
    logger.info 'Stop testing and close the connection to the SUT.'
    return unless @connection

    @connection.close
    @connection = nil
  end

  # Prepare for the next test case.
  def reset
    logger.info 'Reset the connection to the SUT.'
    # Try to reuse the WebSocket connection to the SUT.
    if @connection
      send_reset_to_sut
      send_ready_to_amp
    else
      stop
      start
    end
  end

  # @see super
  def stimulate(label)
    logger.info "Executing stimulus at the SUT: #{label.label}"
    sut_message = label_to_sut_message(label)

    # send confirmation of stimulus back to AMP
    @adapter_core.send_stimulus_confirmation(label, sut_message, Time.now)

    # Send AMQP message to SUT
    DynamosApi.new.stimulate_dynamos(sut_message)
  end

  # @see super
  def supported_labels
    labels = []

    # Map each stimulus to its parameters
    stimulus_parameters = {
      'sql_data_request' => [
        parameter('user', :string),
        parameter('dataProviders', :string),
        parameter('data_request', :string)
      ]
    }

    STIMULI.each do |name|
      params = stimulus_parameters[name] || []
      labels << stimulus(name, params)
    end

    RESPONSES.each { |name| labels << response(name) }

    # extra stimulus to reset the SUT
    labels << stimulus('reset')

    labels
  end

  # The default configuration for this adapter.
  def default_configuration
    url = PluginAdapter::Api::Configuration::Item.new(
      key: 'url',
      description: 'WebSocket URL for the DYNAMOS SUT',
      string: DYNAMOS_URL
    )

    configuration = PluginAdapter::Api::Configuration.new
    configuration.items << url
    configuration
  end

  def send_response_to_amp(message)
    return if message == 'RESET_PERFORMED' # not a real response

    label = sut_message_to_label(message)
    timestamp = Time.now
    physical_label = message
    @adapter_core.send_response(label, physical_label, timestamp)
  end

  def send_error_to_amp(message)
    @adapter_core.send_error(message)
  end

  def send_ready_to_amp
    @adapter_core.send_ready
  end

  def send_reset_to_sut
    reset_string = 'RESET'
    logger.info "Sending '#{reset_string}' to SUT"
    @connection.send(reset_string)
  end

  private

  # Converters

  # Convert a label to a DYNAMOS message
  def label_to_sut_message(label)
    {
      type: label.label,
      parameters: label.parameters.map { |param| [param.name, extract_value(param.value)] }.to_h
    }.to_json
  end

  # Helper function
  def extract_value(value)
    value.string.presence || value.integer.presence || value.boolean.presence
  end

  # Simple factory methods for PluginAdapter::Api objects.

  def stimulus(name, parameters = {}, channel = 'amqp')
    label(name, :STIMULUS, parameters, channel)
  end

  def response(name, parameters = {}, channel = 'amqp')
    label(name, :RESPONSE, parameters, channel)
  end

  def parameter(name, type)
    value = case type
            when :integer
              PluginAdapter::Api::Label::Parameter::Value.new(integer: 0)
            when :string
              PluginAdapter::Api::Label::Parameter::Value.new(string: '')
            else
              raise "#{type} not yet implemented"
            end
    PluginAdapter::Api::Label::Parameter.new(name: name, value: value)
  end

  def label(name, direction, parameters, channel)
    label = PluginAdapter::Api::Label.new
    label.type    = direction
    label.label   = name
    label.channel = channel
    parameters.each { |param| label.parameters << param }
    label
  end
end
