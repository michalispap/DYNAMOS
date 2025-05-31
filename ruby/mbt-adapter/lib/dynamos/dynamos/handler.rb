# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

class DynamosHandler < Handler
  def initialize
    @connection = nil
    super
  end

  STIMULI = %w[sql_data_request].freeze
  RESPONSES = %w[
    results
    requestApproval
    validationResponse
    compositionRequest
    requestApprovalResponse
    microserviceCommunication
    http_response_status
  ].freeze
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

    # Send HTTP request to SUT
    api = DynamosApi.new
    http_call_result = api.stimulate_dynamos(sut_message)
    http_status_code = http_call_result[:code]
    response_body_str = http_call_result[:body_str]

    # Send the http_response_status label to AMP immediately
    status_label = PluginAdapter::Api::Label.new(
      type: :RESPONSE,
      label: "http_response_status",
      channel: "dynamos_channel"
    )
    status_label.parameters << PluginAdapter::Api::Label::Parameter.new(
      name: 'code',
      value: value_to_label_param(http_status_code) # Helper handles type conversion
    )
    status_physical_label = { code: http_status_code }.to_json
    @adapter_core.send_response(status_label, status_physical_label, Time.now)
    logger.info "Sent 'http_response_status' (code: #{http_status_code}) to AMP."

    # Conditionally process and send "results" label
    # Assuming 2xx are successful responses for "results"
    if http_status_code >= 200 && http_status_code < 300
      parsed_results = api.parse_response(response_body_str)
      if parsed_results
        send_response_to_amp(parsed_results) # This existing method handles "results"
      else
        logger.info "HTTP request successful (code: #{http_status_code}), but response body was not in the expected 'results' format or was nil. Not sending 'results' to AMP."
      end
    else
      logger.info "HTTP request was not successful (code: #{http_status_code}). Not attempting to parse for 'results'."
    end
  end

  # @see super
  def supported_labels
    labels = []

    # --- Field Definitions for Reusability ---
    user_fields = {
      'id' => [:string, nil],
      'userName' => [:string, nil]
    }

    data_request_options_fields = { # Used by requestApproval's options
      'graph' => [:boolean, nil],
      'aggregate' => [:boolean, nil]
    }

    data_request_fields = { # For sql_data_request stimulus
      'type' => [:string, nil],
      'query' => [:string, nil],
      'algorithm' => [:string, nil],
      'options' => [:object, data_request_options_fields],
      'requestMetadata' => [:object, {}] # Assuming empty or generic object
    }

    # --- Stimulus Definitions ---
    stimulus_parameters = {
      'sql_data_request' => [
        parameter('user', :object, user_fields),
        parameter('dataProviders', :array, :string), # AMP expects element type for array
        parameter('data_request', :object, data_request_fields)
      ]
    }

    STIMULI.each do |name|
      params = stimulus_parameters[name] || []
      labels << stimulus(name, params)
    end

    # --- Response Definitions ---
    # Define parameters only for the selected fields
    response_parameters = {
      'results' => [
        parameter('jobId', :string),
        parameter('responses', :array, :string) # Array of JSON strings
      ],
      'requestApproval' => [
        parameter('data_providers', :array, :string),
        parameter('options', :object, data_request_options_fields)
      ],
      'validationResponse' => [
        parameter('valid_dataproviders', :array, :string), # Array of names (strings)
        parameter('invalid_dataproviders', :array, :string), # Array of names (strings)
        parameter('request_approved', :boolean)
      ],
      'compositionRequest' => [
        parameter('archetype_id', :string),
        parameter('role', :string),
        parameter('destination_queue', :string)
      ],
      'requestApprovalResponse' => [
        parameter('error', :string) # Assuming error is a simple string
      ],
      'microserviceCommunication' => [
        parameter('return_address', :string)
      ],
      'http_response_status' => [
        parameter('code', :integer)
      ]
    }

    RESPONSES.each do |name|
      params = response_parameters[name] || [] # Use defined params or empty if none
      labels << response(name, params)
    end

    labels << stimulus('reset') # No parameters for reset
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
    # This method is ONLY for HTTP 'results' responses
    return if message == 'RESET_PERFORMED' # not a real response

    # Specific pre-processing for 'results' if its 'responses' field contains complex objects
    if message.is_a?(Hash) && message['responses'].is_a?(Array)
      # Ensure elements of 'responses' are JSON strings if they are not already strings
      message['responses'] = message['responses'].map { |r| r.is_a?(String) ? r : r.to_json }
    end

    label = sut_message_to_label(message) # Expects HTTP response structure for 'results'
    timestamp = Time.now
    physical_label = message.is_a?(String) ? message : message.to_json # For 'results'
    @adapter_core.send_response(label, physical_label, timestamp)
  end

  def process_rabbitmq_message(parsed_data_from_service)
    original_type = parsed_data_from_service[:type]
    payload = parsed_data_from_service[:payload]
    raw_json_body = parsed_data_from_service[:raw_json]

    unless original_type && payload.is_a?(Hash)
      logger.error "DynamosHandler: Invalid data received for RabbitMQ processing. Type: #{original_type.inspect}, Payload Class: #{payload.class}"
      return
    end

    logger.info "DynamosHandler: Processing RabbitMQ message of type '#{original_type}'."
    logger.debug "DynamosHandler: Payload: #{payload.inspect}"

    selected_params = {}
    amp_label_name = original_type

    case original_type
    when 'requestApproval'
      selected_params['data_providers'] = payload['data_providers'] if payload.key?('data_providers')
      selected_params['options'] = payload['options'] if payload.key?('options')
    when 'validationResponse'
      if payload.key?('valid_dataproviders') && payload['valid_dataproviders'].is_a?(Hash)
        selected_params['valid_dataproviders'] = payload['valid_dataproviders'].keys # Array of names
      elsif payload.key?('valid_dataproviders') # If it's already an array
         selected_params['valid_dataproviders'] = Array(payload['valid_dataproviders'])
      end
      selected_params['invalid_dataproviders'] = Array(payload['invalid_dataproviders']) if payload.key?('invalid_dataproviders')
      selected_params['request_approved'] = payload['request_approved'] if payload.key?('request_approved')
    when 'compositionRequest'
      selected_params['archetype_id'] = payload['archetype_id'] if payload.key?('archetype_id')
      selected_params['role'] = payload['role'] if payload.key?('role')
      selected_params['destination_queue'] = payload['destination_queue'] if payload.key?('destination_queue')
    when 'requestApprovalResponse'
      selected_params['error'] = payload['error'] if payload.key?('error') # Assuming 'error' is a top-level string
    when 'microserviceCommunication'
      selected_params['return_address'] = payload.dig('request_metadata', 'return_address')
    else
      logger.warn "DynamosHandler: No specific parameter selection defined for RabbitMQ message type '#{original_type}'. Not sending to AMP."
      return # Do not send if type is not explicitly handled for selection
    end
    
    logger.info "DynamosHandler: Selected parameters for '#{amp_label_name}': #{selected_params.inspect}"

    # Construct the Label object
    label_to_send = PluginAdapter::Api::Label.new(
      type: :RESPONSE,
      label: amp_label_name.to_s,
      channel: 'dynamos_channel' # Assuming all RabbitMQ messages use this channel
    )

    selected_params.each do |name, value|
      # Skip adding parameter if value is nil and you don't want to send nil parameters
      # next if value.nil? # Optional: uncomment if you want to omit nil parameters
      label_to_send.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: name.to_s,
        value: value_to_label_param(value) # value_to_label_param expects pure Ruby types
      )
    end
    
    # Send to AMP via AdapterCore
    @adapter_core.send_response(label_to_send, raw_json_body, Time.now)
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

  def sut_message_to_label(message)
    # This method is specifically for the 'results' (HTTP) response
    label = PluginAdapter::Api::Label.new
    label.type = :RESPONSE
    label.label = "results" # Hardcoded for HTTP responses
    label.channel = "dynamos_channel"

    # Ensure message is a hash
    unless message.is_a?(Hash)
      logger.warn "sut_message_to_label expected a Hash for 'results', got #{message.class}. Creating empty label."
      return label
    end

    # Specific handling for 'results' structure
    if message.key?('jobId')
      label.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: 'jobId',
        value: value_to_label_param(message['jobId'])
      )
    end
    if message.key?('responses') && message['responses'].is_a?(Array)
      # 'responses' for 'results' are expected to be an array of JSON strings by AMP model
      # value_to_label_param will handle array of strings.
      label.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: 'responses',
        value: value_to_label_param(message['responses'])
      )
    end
    label
  end

  def value_to_label_param(obj)
    # This method expects pure Ruby types due to dynamos_object_to_hash
    logger.debug "DynamosHandler#value_to_label_param: obj class: #{obj.class}, obj inspect: #{obj.inspect}"
    case obj
    when Hash
      entries = obj.map do |k, v|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: k.to_s), # Keys are always strings for AMP struct
          value: value_to_label_param(v) # Recursive call for value
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )
    when Array
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: obj.map { |v| value_to_label_param(v) } # Recursive call for each element
        )
      )
    when String
      PluginAdapter::Api::Label::Parameter::Value.new(string: obj)
    when Integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: obj)
    when Float
      PluginAdapter::Api::Label::Parameter::Value.new(decimal: obj) # AMP uses 'decimal' for float/double
    when TrueClass, FalseClass
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: obj)
    when NilClass
      PluginAdapter::Api::Label::Parameter::Value.new # Empty value represents nil
    else
      logger.warn "DynamosHandler#value_to_label_param: Unhandled Ruby type #{obj.class}, converting to string: #{obj.inspect}"
      PluginAdapter::Api::Label::Parameter::Value.new(string: obj.to_s) # Fallback
    end
  end

  # Simple factory methods for PluginAdapter::Api objects.

  def stimulus(name, parameters = {}, channel = 'dynamos_channel')
    label(name, :STIMULUS, parameters, channel)
  end

  def response(name, parameters = {}, channel = 'dynamos_channel')
    label(name, :RESPONSE, parameters, channel)
  end

  def parameter(name, type, fields = nil)
    PluginAdapter::Api::Label::Parameter.new(name: name, value: build_value(type, fields))
  end

  def build_value(type, fields_or_element_type = nil)
    case type
    when :integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: 0)
    when :string
      PluginAdapter::Api::Label::Parameter::Value.new(string: '')
    when :boolean
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: false)
    when :array
      # For array, fields_or_element_type specifies the type of elements in the array
      element_type_symbol = fields_or_element_type.is_a?(Symbol) ? fields_or_element_type : :string # Default to string
      element_dummy_value = build_value(element_type_symbol) # Recursive call for element type
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: [element_dummy_value] # AMP expects a dummy value for array elements
        )
      )
    when :object # Used for 'struct' in AMP
      entries = (fields_or_element_type || {}).map do |field_name, (field_type_symbol, subfields_hash)|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: field_name),
          value: build_value(field_type_symbol, subfields_hash) # Recursive call
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )

    else
      raise "#{type} not yet implemented in build_value"
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
