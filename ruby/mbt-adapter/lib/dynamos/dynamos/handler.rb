# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Main DYNAMOS adapter logic.
# Handles SUT lifecycle, HTTP/RabbitMQ, and defines adapter interface.
class DynamosHandler < Handler
  def initialize
    @connection = nil
    super
  end

  STIMULI = %w[sql_data_request switch_archetype].freeze
  RESPONSES = %w[
    results
    requestApproval
    validationResponse
    compositionRequest
    requestApprovalResponse
    microserviceCommunication
    http_response_status
    anonymizeFinished
    algorithmFinished
    aggregateFinished
    queryFinished
    sqlDataRequest
  ].freeze
  private_constant :STIMULI, :RESPONSES

  DYNAMOS_URL = 'ws://127.0.0.1:3001'

  # Connects to SUT (RabbitMQ) for test session.
  def start
    logger.debug "Starting connection. Connection is nil? -> #{@connection.nil?}"
    return unless @connection.nil?
    logger.info 'Starting. Trying to connect to the SUT.'
    @connection = DynamosConnection.new(self)
    @connection.connect
  end

  # Closes SUT connection.
  def stop
    logger.info 'Stop testing and close the connection to the SUT.'
    return unless @connection
    @connection.close
    @connection = nil
  end

  # Prepares for next test case (reuses or reconnects).
  def reset
    logger.info 'Reset the connection to the SUT.'
    if @connection
      send_reset_to_sut
      send_ready_to_amp
    else
      stop
      start
    end
  end

  # Handles stimulus from AMP, sends to SUT.
  def stimulate(label)
    logger.info "Executing stimulus at the SUT: #{label.label}"
    api = DynamosApi.new
    http_call_result = nil

    case label.label
    when 'sql_data_request'
      sut_message = label_to_sut_message(label)
      logger.info "Generated SUT message: #{sut_message}"
      @adapter_core.send_stimulus_confirmation(label, sut_message, Time.now)
      http_call_result = api.stimulate_dynamos(sut_message)
    when 'switch_archetype'
      sut_message = label_to_sut_message(label)
      logger.info "Generated SUT message: #{sut_message}"
      @adapter_core.send_stimulus_confirmation(label, sut_message, Time.now)
      http_call_result = api.switch_archetype(sut_message)
    else
      logger.error "Unknown stimulus: #{label.label}"
      return
    end

    http_status_code = http_call_result[:code]
    response_body_str = http_call_result[:body_str]

    status_label = PluginAdapter::Api::Label.new(
      type: :RESPONSE,
      label: "http_response_status",
      channel: "dynamos_channel"
    )
    status_label.parameters << PluginAdapter::Api::Label::Parameter.new(
      name: 'code',
      value: value_to_label_param(http_status_code)
    )
    status_physical_label = { code: http_status_code }.to_json
    @adapter_core.send_response(status_label, status_physical_label, Time.now)
    logger.info "Sent 'http_response_status' (code: #{http_status_code}) to AMP."

    return if http_status_code == 0

    if http_status_code >= 200 && http_status_code < 300
      parsed_results = api.parse_response(response_body_str)
      send_response_to_amp(parsed_results) if parsed_results
    end
  end

  # Returns supported labels for AMP.
  def supported_labels
    labels = []

    user_fields = {
      'id' => [:string, nil],
      'userName' => [:string, nil]
    }
    data_request_options_fields = {
      'graph' => [:boolean, nil],
      'aggregate' => [:boolean, nil],
      'anonymize' => [:boolean, nil]
    }
    data_request_fields = {
      'type' => [:string, nil],
      'query' => [:string, nil],
      'algorithm' => [:string, nil],
      'options' => [:object, data_request_options_fields],
      'requestMetadata' => [:object, {}]
    }

    stimulus_parameters = {
      'sql_data_request' => [
        parameter('request_type', :string),
        parameter('user', :object, user_fields),
        parameter('dataProviders', :array, :string),
        parameter('data_request', :object, data_request_fields)
      ],
      'switch_archetype' => [
        parameter('weight', :integer)
      ]
    }
    STIMULI.each do |name|
      params = stimulus_parameters[name] || []
      labels << stimulus(name, params)
    end

    response_parameters = {
      'results' => [
        parameter('jobId', :string),
        parameter('responses', :array, :string)
      ],
      'sqlDataRequest' => [
        parameter('query', :string)
      ],
      'requestApproval' => [
        parameter('data_providers', :array, :string),
        parameter('options', :object, data_request_options_fields)
      ],
      'validationResponse' => [
        parameter('valid_dataproviders', :array, :string),
        parameter('invalid_dataproviders', :array, :string),
        parameter('request_approved', :boolean)
      ],
      'compositionRequest' => [
        parameter('archetype_id', :string),
        parameter('role', :string),
        parameter('destination_queue', :string)
      ],
      'requestApprovalResponse' => [
        parameter('error', :string)
      ],
      'microserviceCommunication' => [
        parameter('return_address', :string),
        parameter('result', :string)
      ],
      'anonymizeFinished' => [],
      'algorithmFinished' => [],
      'aggregateFinished' => [],
      'queryFinished' => [],
      'http_response_status' => [
        parameter('code', :integer)
      ]
    }
    RESPONSES.each do |name|
      params = response_parameters[name] || []
      labels << response(name, params)
    end

    labels << stimulus('reset')
    labels
  end

  # Returns default config for adapter.
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

  # Sends "results" response to AMP.
  # message: SUT "results" hash.
  def send_response_to_amp(message)
    return if message == 'RESET_PERFORMED'
    if message.is_a?(Hash) && message['responses'].is_a?(Array)
      message['responses'] = message['responses'].map { |r| r.is_a?(String) ? r : r.to_json }
    end
    send_amp_label("results", message)
  end

  # Handles incoming RabbitMQ message, sends to AMP if valid.
  def process_rabbitmq_message(parsed_data_from_service)
    original_type = parsed_data_from_service[:type]
    payload = parsed_data_from_service[:payload]
    raw_json_body = parsed_data_from_service[:raw_json]

    unless original_type && payload.is_a?(Hash)
      logger.error "DynamosHandler: Invalid data from RabbitMQService. Type: #{original_type.inspect}, Payload Class: #{payload.class}"
      return
    end
    logger.info "DynamosHandler: Processing RabbitMQ message type '#{original_type}'."

    selected_params = extract_amp_params(original_type, payload)
    unless selected_params
      logger.warn "DynamosHandler: No parameter selection for RabbitMQ type '#{original_type}'. Not sending to AMP."
      return
    end

    logger.info "DynamosHandler: Selected parameters for '#{original_type}': #{selected_params.inspect}"
    send_amp_label(original_type, raw_json_body, selected_params)
  end

  # Sends error string to AMP.
  def send_error_to_amp(message)
    @adapter_core.send_error(message)
  end

  # Tells AMP we're ready.
  def send_ready_to_amp
    @adapter_core.send_ready
  end

  # Sends "RESET" to SUT via RabbitMQ.
  def send_reset_to_sut
    reset_string = 'RESET'
    logger.info "Sending '#{reset_string}' to SUT"
    @connection.send(reset_string)
  end

  private

  # Sends label to AMP, builds params if needed.
  def send_amp_label(label_name, physical_label, params_hash = nil)
    label_to_send = PluginAdapter::Api::Label.new(
      type: :RESPONSE,
      label: label_name.to_s,
      channel: 'dynamos_channel'
    )
    if params_hash
      params_hash.each do |name, value|
        label_to_send.parameters << PluginAdapter::Api::Label::Parameter.new(
          name: name.to_s,
          value: value_to_label_param(value)
        )
      end
    else
      # For "results" label, extract params from message hash
      if physical_label.is_a?(Hash)
        %w[jobId responses].each do |key|
          if physical_label.key?(key)
            label_to_send.parameters << PluginAdapter::Api::Label::Parameter.new(
              name: key,
              value: value_to_label_param(physical_label[key])
            )
          end
        end
      end
    end
    timestamp = Time.now
    physical = physical_label.is_a?(String) ? physical_label : physical_label.to_json
    @adapter_core.send_response(label_to_send, physical, timestamp)
  end

  # Extracts AMP params from RabbitMQ payload.
  def extract_amp_params(type, payload)
    case type
    when 'requestApproval'
      {
        'data_providers' => payload['data_providers'],
        'options' => payload['options']
      }.compact
    when 'sqlDataRequest'
      { 'query' => payload['query'] }.compact
    when 'validationResponse'
      valid = if payload.key?('valid_dataproviders') && payload['valid_dataproviders'].is_a?(Hash)
        payload['valid_dataproviders'].keys
      elsif payload.key?('valid_dataproviders')
        Array(payload['valid_dataproviders'])
      end
      {
        'valid_dataproviders' => valid,
        'invalid_dataproviders' => Array(payload['invalid_dataproviders']),
        'request_approved' => payload['request_approved']
      }.compact
    when 'compositionRequest'
      {
        'archetype_id' => payload['archetype_id'],
        'role' => payload['role'],
        'destination_queue' => payload['destination_queue']
      }.compact
    when 'requestApprovalResponse'
      { 'error' => payload['error'] }.compact
    when 'microserviceCommunication'
      {
        'return_address' => payload.dig('request_metadata', 'return_address'),
        'result' => payload['result']
      }.compact
    when 'anonymizeFinished', 'algorithmFinished', 'aggregateFinished', 'queryFinished'
      {}
    else
      nil
    end
  end

  # --- Converters: AMP Label <-> SUT Message ---

  # Converts AMP label to SUT JSON string.
  def label_to_sut_message(label)
    params_hash = label.parameters.map { |param| [param.name, extract_value(param.value)] }.to_h

    case label.label
    when 'sql_data_request'
      sut_top_level_type = params_hash.delete('request_type') || 'sqlDataRequest'
      sut_payload = params_hash
      { type: sut_top_level_type }.merge(sut_payload).to_json
    when 'switch_archetype'
      {
        name: 'computeToData',
        computeProvider: 'dataProvider',
        resultRecipient: 'requestor',
        weight: params_hash['weight']
      }.to_json
    else
      sut_top_level_type = label.label
      sut_payload = params_hash
      { type: sut_top_level_type }.merge(sut_payload).to_json
    end
  end

  # Recursively extracts Ruby value from AMP param.
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

  # Converts SUT "results" hash to AMP label.
  def sut_message_to_label(message)
    label = PluginAdapter::Api::Label.new
    label.type = :RESPONSE
    label.label = "results"
    label.channel = "dynamos_channel"

    unless message.is_a?(Hash)
      logger.warn "sut_message_to_label expected Hash for 'results', got #{message.class}."
      return label
    end

    if message.key?('jobId')
      label.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: 'jobId',
        value: value_to_label_param(message['jobId'])
      )
    end
    if message.key?('responses') && message['responses'].is_a?(Array)
      label.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: 'responses',
        value: value_to_label_param(message['responses'])
      )
    end
    label
  end

  # Converts Ruby object to AMP param (protobuf).
  def value_to_label_param(obj)
    logger.debug "DynamosHandler#value_to_label_param: obj class: #{obj.class}, obj inspect: #{obj.inspect}"
    case obj
    when Hash
      entries = obj.map do |k, v|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: k.to_s),
          value: value_to_label_param(v)
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )
    when Array
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: obj.map { |v| value_to_label_param(v) }
        )
      )
    when String
      PluginAdapter::Api::Label::Parameter::Value.new(string: obj)
    when Integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: obj)
    when Float
      PluginAdapter::Api::Label::Parameter::Value.new(decimal: obj)
    when TrueClass, FalseClass
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: obj)
    when NilClass
      PluginAdapter::Api::Label::Parameter::Value.new
    else
      logger.warn "DynamosHandler#value_to_label_param: Unhandled Ruby type #{obj.class}, converting to string: #{obj.inspect}"
      PluginAdapter::Api::Label::Parameter::Value.new(string: obj.to_s)
    end
  end

  # --- Factory methods for supported_labels ---

  # Defines a stimulus label.
  def stimulus(name, parameters = {}, channel = 'dynamos_channel')
    label(name, :STIMULUS, parameters, channel)
  end

  # Defines a response label.
  def response(name, parameters = {}, channel = 'dynamos_channel')
    label(name, :RESPONSE, parameters, channel)
  end

  # Defines a parameter for supported_labels.
  def parameter(name, type, fields = nil)
    PluginAdapter::Api::Label::Parameter.new(name: name, value: build_value(type, fields))
  end

  # Builds dummy AMP param value for supported_labels.
  def build_value(type, fields_or_element_type = nil)
    case type
    when :integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: 0)
    when :string
      PluginAdapter::Api::Label::Parameter::Value.new(string: '')
    when :boolean
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: false)
    when :array
      element_type_symbol = fields_or_element_type.is_a?(Symbol) ? fields_or_element_type : :string
      element_dummy_value = build_value(element_type_symbol)
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: [element_dummy_value]
        )
      )
    when :object
      entries = (fields_or_element_type || {}).map do |field_name, (field_type_symbol, subfields_hash)|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: field_name),
          value: build_value(field_type_symbol, subfields_hash)
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )
    else
      raise "#{type} not yet implemented in build_value"
    end
  end

  # Helper to create label definition.
  def label(name, direction, parameters, channel)
    label = PluginAdapter::Api::Label.new
    label.type    = direction
    label.label   = name
    label.channel = channel
    parameters.each { |param| label.parameters << param }
    label
  end
end
