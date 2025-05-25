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

    logger.info "AMP provided label: #{label.inspect}"
    sut_message = label_to_sut_message(label)
    logger.info "Generated SUT message: #{sut_message}"

    # Send confirmation of stimulus back to AMP
    @adapter_core.send_stimulus_confirmation(label, sut_message, Time.now)

    # Send AMQP message to SUT
    DynamosApi.new.stimulate_dynamos(sut_message)
  end

  # @see super
  def supported_labels
    labels = []

    user_fields = {
      'id' => [:string, nil],
      'userName' => [:string, nil]
    }

    data_request_fields = {
      'type' => [:string, nil],
      'query' => [:string, nil],
      'algorithm' => [:string, nil],
      'options' => [
        :object, {
        'graph' => [:boolean, nil],
        'aggregate' => [:boolean, nil]
      }
      ],
      'requestMetadata' => [:object, {}]
    }

    stimulus_parameters = {
      'sql_data_request' => [
        parameter('user', :object, user_fields),
        parameter('dataProviders', :array, nil),
        parameter('data_request', :object, data_request_fields)
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
    params = label.parameters.map { |param| [param.name, extract_value(param.value)] }.to_h
    type = label.label == 'sql_data_request' ? 'sqlDataRequest' : label.label
    { type: type }.merge(params).to_json
  end

  # Helper function
  def extract_value(value)
    return value.string if value.respond_to?(:has_string?) && value.has_string?
    return value.integer if value.respond_to?(:has_integer?) && value.has_integer?
    return value.decimal if value.respond_to?(:has_decimal?) && value.has_decimal?
    return value.boolean if value.respond_to?(:has_boolean?) && value.has_boolean?
    return value.date if value.respond_to?(:has_date?) && value.has_date?
    return value.time if value.respond_to?(:has_time?) && value.has_time?
    if value.respond_to?(:has_array?) && value.has_array?
      return value.array.values.map { |v| extract_value(v) }
    end
    if value.respond_to?(:has_struct?) && value.has_struct?
      return value.struct.entries.each_with_object({}) do |entry, h|
        h[extract_value(entry.key)] = extract_value(entry.value)
      end
    end
    if value.respond_to?(:has_hash_value?) && value.has_hash_value?
      return value.hash_value.entries.each_with_object({}) do |entry, h|
        h[extract_value(entry.key)] = extract_value(entry.value)
      end
    end
    nil
  end

  # Simple factory methods for PluginAdapter::Api objects.

  def stimulus(name, parameters = {}, channel = 'amqp')
    label(name, :STIMULUS, parameters, channel)
  end

  def response(name, parameters = {}, channel = 'amqp')
    label(name, :RESPONSE, parameters, channel)
  end

  def parameter(name, type, fields = nil)
    PluginAdapter::Api::Label::Parameter.new(name: name, value: build_value(type, fields))
  end

  def build_value(type, fields = nil)
    case type
    when :integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: 0)
    when :string
      PluginAdapter::Api::Label::Parameter::Value.new(string: '')
    when :boolean
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: false)
    when :array
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: [PluginAdapter::Api::Label::Parameter::Value.new(string: '')]
        )
      )
    when :object
      entries = (fields || {}).map do |field_name, (field_type, subfields)|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: field_name),
          value: build_value(field_type, subfields)
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )
    else
      raise "#{type} not yet implemented"
    end
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
