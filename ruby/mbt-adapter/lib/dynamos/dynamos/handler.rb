# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Core logic for the DYNAMOS adapter.
# Handles AMP lifecycle, SUT interactions (HTTP & RabbitMQ), and defines adapter interface.
class DynamosHandler < Handler
  def initialize
    @connection = nil # Manages SUT connection (RabbitMQ).
    super
  end

  # Supported stimulus and response label names for AMP.
  STIMULI = %w[sql_data_request].freeze
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
  ].freeze
  private_constant :STIMULI, :RESPONSES

  DYNAMOS_URL = 'ws://127.0.0.1:3001' # SUT URL.

  # Prepares adapter for test session: connects to SUT (RabbitMQ).
  def start
    logger.debug "Starting connection. Connection is nil? -> #{@connection.nil?}"
    return unless @connection.nil?

    logger.info 'Starting. Trying to connect to the SUT.'
    @connection = DynamosConnection.new(self)
    @connection.connect
    # DynamosConnection's on_connected callback signals AMP when ready.
  end

  # Ends test session: closes SUT connection.
  def stop
    logger.info 'Stop testing and close the connection to the SUT.'
    return unless @connection

    @connection.close
    @connection = nil
  end

  # Prepares for next test case: reuses SUT connection if possible.
  def reset
    logger.info 'Reset the connection to the SUT.'
    if @connection
      send_reset_to_sut # Send RESET to SUT (RabbitMQ).
      send_ready_to_amp # Signal AMP adapter is ready.
    else
      # If no connection, perform full stop and start.
      stop
      start
    end
  end

  # Handles stimulus from AMP: sends request to SUT.
  def stimulate(label)
    logger.info "Executing stimulus at the SUT: #{label.label}"
    sut_message = label_to_sut_message(label) # Convert AMP Label to SUT JSON.
    logger.info "Generated SUT message: #{sut_message}"

    # Confirm to AMP that stimulus is being processed.
    @adapter_core.send_stimulus_confirmation(label, sut_message, Time.now)

    # --- SUT HTTP Interaction ---
    api = DynamosApi.new
    http_call_result = api.stimulate_dynamos(sut_message) # HTTP POST to SUT.
    http_status_code = http_call_result[:code]
    response_body_str = http_call_result[:body_str]

    # Always report HTTP status code to AMP, even for network/client errors.
    status_label = PluginAdapter::Api::Label.new(
      type: :RESPONSE,
      label: "http_response_status",
      channel: "dynamos_channel"
    )
    status_label.parameters << PluginAdapter::Api::Label::Parameter.new(
      name: 'code',
      value: value_to_label_param(http_status_code) # Convert Ruby int to AMP param.
    )
    status_physical_label = { code: http_status_code }.to_json
    @adapter_core.send_response(status_label, status_physical_label, Time.now)
    logger.info "Sent 'http_response_status' (code: #{http_status_code}) to AMP."

    if http_status_code == 0
      logger.warn "HTTP request failed due to network/client error. Code 0 sent to AMP."
      # Do not attempt to parse results if network error.
      return
    end

    # Conditionally process SUT's HTTP response body for "results".
    if http_status_code >= 200 && http_status_code < 300 # Check for 2xx success.
      parsed_results = api.parse_response(response_body_str) # Parse for "results".
      if parsed_results
        send_response_to_amp(parsed_results) # Send "results" label to AMP if valid.
      else
        logger.info "HTTP request successful (code: #{http_status_code}), but 'results' format invalid/nil. Not sending 'results' to AMP."
      end
    else
      logger.info "HTTP request not successful (code: #{http_status_code}). Not parsing for 'results'."
    end
  end

  # Defines adapter's interface (supported labels) to AMP.
  def supported_labels
    labels = []

    # Reusable field definitions for certain AMP label parameters.
    user_fields = {
      'id' => [:string, nil],
      'userName' => [:string, nil]
    }
    data_request_options_fields = { # For 'data_request' & 'requestApproval'.
      'graph' => [:boolean, nil],
      'aggregate' => [:boolean, nil],
      'anonymize' => [:boolean, nil]
    }
    data_request_fields = { # For 'data_request' object in 'sql_data_request'.
      'type' => [:string, nil], # 'type' within 'data_request' object.
      'query' => [:string, nil],
      'algorithm' => [:string, nil],
      'options' => [:object, data_request_options_fields],
      'requestMetadata' => [:object, {}] # Empty/generic object.
    }

    # Stimulus label definitions.
    stimulus_parameters = {
      'sql_data_request' => [
        parameter('request_type', :string), # Top-level 'type' for SUT JSON.
        parameter('user', :object, user_fields),
        parameter('dataProviders', :array, :string), # Array of strings.
        parameter('data_request', :object, data_request_fields)
      ]
    }
    STIMULI.each do |name|
      params = stimulus_parameters[name] || [] # Default to no params.
      labels << stimulus(name, params) # `stimulus` is a factory method.
    end

    # Response label definitions.
    response_parameters = {
      'results' => [
        parameter('jobId', :string),
        parameter('responses', :array, :string) # Array of JSON strings.
      ],
      'requestApproval' => [
        parameter('data_providers', :array, :string),
        parameter('options', :object, data_request_options_fields)
      ],
      'validationResponse' => [
        parameter('valid_dataproviders', :array, :string), # Array of provider names.
        parameter('invalid_dataproviders', :array, :string), # Array of provider names.
        parameter('request_approved', :boolean)
      ],
      'compositionRequest' => [
        parameter('archetype_id', :string),
        parameter('role', :string),
        parameter('destination_queue', :string)
      ],
      'requestApprovalResponse' => [
        parameter('error', :string) # Simple string error.
      ],
      'microserviceCommunication' => [
        parameter('return_address', :string),
        parameter('result', :string)
      ],
      'anonymizeFinished' => [],
      'algorithmFinished' => [],
      'aggregateFinished' => [],
      'http_response_status' => [
        parameter('code', :integer)
      ]
    }
    RESPONSES.each do |name|
      params = response_parameters[name] || []
      labels << response(name, params) # `response` is a factory method.
    end

    labels << stimulus('reset') # 'reset' stimulus has no parameters.
    labels
  end

  # Defines default adapter configuration.
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

  # Sends "results" response (from SUT HTTP call) to AMP.
  # @param message [Hash] SUT "results" message.
  def send_response_to_amp(message)
    # Specific to HTTP "results" responses.
    return if message == 'RESET_PERFORMED' # Ignore internal reset signal.

    # Ensure 'responses' elements are JSON strings if complex.
    if message.is_a?(Hash) && message['responses'].is_a?(Array)
      message['responses'] = message['responses'].map { |r| r.is_a?(String) ? r : r.to_json }
    end

    label = sut_message_to_label(message) # Convert SUT "results" hash to AMP Label.
    timestamp = Time.now
    physical_label = message.is_a?(String) ? message : message.to_json
    @adapter_core.send_response(label, physical_label, timestamp)
  end

  # Processes messages from RabbitMQ and forwards to AMP.
  # @param parsed_data_from_service [Hash] Contains :type, :payload (Hash), :raw_json.
  def process_rabbitmq_message(parsed_data_from_service)
    original_type = parsed_data_from_service[:type]     # e.g., "validationResponse"
    payload = parsed_data_from_service[:payload]         # Decoded Ruby hash from Protobuf.
    raw_json_body = parsed_data_from_service[:raw_json]  # Original JSON from RabbitMQ.

    unless original_type && payload.is_a?(Hash) # Basic validation.
      logger.error "DynamosHandler: Invalid data from RabbitMQService. Type: #{original_type.inspect}, Payload Class: #{payload.class}"
      return
    end
    logger.info "DynamosHandler: Processing RabbitMQ message type '#{original_type}'."

    # Select/transform parameters from payload for AMP.
    selected_params = {}
    amp_label_name = original_type # AMP label name usually matches RabbitMQ type.

    case original_type
    when 'requestApproval'
      selected_params['data_providers'] = payload['data_providers'] if payload.key?('data_providers')
      selected_params['options'] = payload['options'] if payload.key?('options')
    when 'validationResponse'
      # 'valid_dataproviders' might be hash (keys are names) or array.
      if payload.key?('valid_dataproviders') && payload['valid_dataproviders'].is_a?(Hash)
        selected_params['valid_dataproviders'] = payload['valid_dataproviders'].keys
      elsif payload.key?('valid_dataproviders') # If already array or other.
         selected_params['valid_dataproviders'] = Array(payload['valid_dataproviders'])
      end
      selected_params['invalid_dataproviders'] = Array(payload['invalid_dataproviders']) if payload.key?('invalid_dataproviders')
      selected_params['request_approved'] = payload['request_approved'] if payload.key?('request_approved')
    when 'compositionRequest'
      selected_params['archetype_id'] = payload['archetype_id'] if payload.key?('archetype_id')
      selected_params['role'] = payload['role'] if payload.key?('role')
      selected_params['destination_queue'] = payload['destination_queue'] if payload.key?('destination_queue')
    when 'requestApprovalResponse'
      selected_params['error'] = payload['error'] if payload.key?('error')
    when 'microserviceCommunication'
      # Dig into nested structure for 'return_address'.
      selected_params['return_address'] = payload.dig('request_metadata', 'return_address')
      selected_params['result'] = payload['result'] if payload.key?('result')
    when 'anonymizeFinished', 'algorithmFinished', 'aggregateFinished'
      # These are notifications that a step is finished. No parameters needed.
    else
      # If type not explicitly handled, don't send to AMP.
      logger.warn "DynamosHandler: No parameter selection for RabbitMQ type '#{original_type}'. Not sending to AMP."
      return
    end
    
    logger.info "DynamosHandler: Selected parameters for '#{amp_label_name}': #{selected_params.inspect}"

    # Construct AMP Label object.
    label_to_send = PluginAdapter::Api::Label.new(
      type: :RESPONSE,
      label: amp_label_name.to_s,
      channel: 'dynamos_channel' # Assuming all RabbitMQ messages use this AMP channel.
    )
    selected_params.each do |name, value|
      # `value_to_label_param` converts Ruby types to AMP Protobuf Value.
      label_to_send.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: name.to_s,
        value: value_to_label_param(value)
      )
    end
    
    # Send constructed label and original raw JSON to AMP.
    @adapter_core.send_response(label_to_send, raw_json_body, Time.now)
  end

  # Sends error message string to AMP.
  def send_error_to_amp(message)
    @adapter_core.send_error(message)
  end

  # Signals AMP that adapter is ready for testing.
  def send_ready_to_amp
    @adapter_core.send_ready
  end

  # Sends "RESET" command to SUT (via RabbitMQ).
  def send_reset_to_sut
    reset_string = 'RESET'
    logger.info "Sending '#{reset_string}' to SUT"
    @connection.send(reset_string)
  end

  private

  # --- Converters: AMP Label <-> SUT Message ---

  # Converts AMP Label ('sql_data_request' stimulus) to JSON string for DYNAMOS SUT.
  def label_to_sut_message(label)
    # Extract all AMP Label parameters into a Ruby hash.
    params_hash = label.parameters.map { |param| [param.name, extract_value(param.value)] }.to_h

    sut_top_level_type = nil
    sut_payload = {}

    if label.label == 'sql_data_request'
      # Use 'request_type' param from AMP for top-level 'type' in SUT JSON.
      sut_top_level_type = params_hash.delete('request_type')
      # Fallback if 'request_type' is missing in model.
      unless sut_top_level_type
        logger.warn "DynamosHandler: 'request_type' param missing for 'sql_data_request'. Defaulting to 'sqlDataRequest'."
        sut_top_level_type = 'sqlDataRequest' # Default if not specified by AMP.
      end
      # Remaining params (user, dataProviders, data_request) form the payload.
      sut_payload = params_hash
    else
      # Fallback for other (currently undefined) stimuli.
      sut_top_level_type = label.label
      sut_payload = params_hash
    end
    # Construct final SUT message:
    # e.g., { "type": "sqlDataRequest", "user": {...}, ... }
    { type: sut_top_level_type }.merge(sut_payload).to_json
  end

  # Recursively extracts Ruby values from AMP Label::Parameter::Value objects.
  def extract_value(value)
    return value.string if value.respond_to?(:has_string?) && value.has_string?
    return value.integer if value.respond_to?(:has_integer?) && value.has_integer?
    return value.decimal if value.respond_to?(:has_decimal?) && value.has_decimal?
    return value.boolean if value.respond_to?(:has_boolean?) && value.has_boolean?
    return value.date if value.respond_to?(:has_date?) && value.has_date?
    return value.time if value.respond_to?(:has_time?) && value.has_time?
    if value.respond_to?(:has_array?) && value.has_array?
      return value.array.values.map { |v| extract_value(v) } # Recurse for array elements.
    end
    if value.respond_to?(:has_struct?) && value.has_struct?
      # Convert AMP struct (object with fixed keys) to Ruby hash.
      return value.struct.entries.each_with_object({}) do |entry, h|
        h[extract_value(entry.key)] = extract_value(entry.value)
      end
    end
    if value.respond_to?(:has_hash_value?) && value.has_hash_value?
      # Convert AMP hash (object with variable keys) to Ruby hash.
      return value.hash_value.entries.each_with_object({}) do |entry, h|
        h[extract_value(entry.key)] = extract_value(entry.value)
      end
    end
    nil # Default if type not set or recognized.
  end

  # Converts SUT "results" message (Ruby hash) to AMP Label object.
  def sut_message_to_label(message)
    # Specific to "results" HTTP response structure.
    label = PluginAdapter::Api::Label.new
    label.type = :RESPONSE
    label.label = "results" # Label name fixed for this conversion.
    label.channel = "dynamos_channel"

    unless message.is_a?(Hash) # Basic validation.
      logger.warn "sut_message_to_label expected Hash for 'results', got #{message.class}."
      return label # Return empty label on invalid input.
    end

    # Populate params based on expected "results" fields.
    if message.key?('jobId')
      label.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: 'jobId',
        value: value_to_label_param(message['jobId'])
      )
    end
    if message.key?('responses') && message['responses'].is_a?(Array)
      # 'responses' for "results" is array of JSON strings for AMP model.
      # `value_to_label_param` handles arrays of strings.
      label.parameters << PluginAdapter::Api::Label::Parameter.new(
        name: 'responses',
        value: value_to_label_param(message['responses'])
      )
    end
    label
  end

  # Recursively converts Ruby objects to AMP Label::Parameter::Value objects (Protobuf).
  # Used when constructing labels to send to AMP.
  def value_to_label_param(obj)
    logger.debug "DynamosHandler#value_to_label_param: obj class: #{obj.class}, obj inspect: #{obj.inspect}"
    case obj
    when Hash
      # Convert Ruby hash to AMP struct (object with fixed keys).
      entries = obj.map do |k, v|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: k.to_s), # Struct keys are strings.
          value: value_to_label_param(v) # Recurse for value.
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )
    when Array
      # Convert Ruby array to AMP array.
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: obj.map { |v| value_to_label_param(v) } # Recurse for elements.
        )
      )
    when String
      PluginAdapter::Api::Label::Parameter::Value.new(string: obj)
    when Integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: obj)
    when Float
      PluginAdapter::Api::Label::Parameter::Value.new(decimal: obj) # AMP uses 'decimal' for floats.
    when TrueClass, FalseClass
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: obj)
    when NilClass
      PluginAdapter::Api::Label::Parameter::Value.new # Empty Value represents nil.
    else
      # Fallback for unhandled Ruby types.
      logger.warn "DynamosHandler#value_to_label_param: Unhandled Ruby type #{obj.class}, converting to string: #{obj.inspect}"
      PluginAdapter::Api::Label::Parameter::Value.new(string: obj.to_s)
    end
  end

  # --- Factory methods for defining labels/params in `supported_labels` ---

  # Creates stimulus label definition.
  def stimulus(name, parameters = {}, channel = 'dynamos_channel')
    label(name, :STIMULUS, parameters, channel)
  end

  # Creates response label definition.
  def response(name, parameters = {}, channel = 'dynamos_channel')
    label(name, :RESPONSE, parameters, channel)
  end

  # Creates parameter definition for `supported_labels`.
  def parameter(name, type, fields = nil)
    PluginAdapter::Api::Label::Parameter.new(name: name, value: build_value(type, fields))
  end

  # Builds dummy AMP Parameter::Value for `supported_labels` definitions.
  def build_value(type, fields_or_element_type = nil)
    case type
    when :integer
      PluginAdapter::Api::Label::Parameter::Value.new(integer: 0)
    when :string
      PluginAdapter::Api::Label::Parameter::Value.new(string: '')
    when :boolean
      PluginAdapter::Api::Label::Parameter::Value.new(boolean: false)
    when :array
      # `fields_or_element_type` is element type for array.
      element_type_symbol = fields_or_element_type.is_a?(Symbol) ? fields_or_element_type : :string # Default string.
      element_dummy_value = build_value(element_type_symbol) # Dummy element.
      PluginAdapter::Api::Label::Parameter::Value.new(
        array: PluginAdapter::Api::Label::Parameter::Value::Array.new(
          values: [element_dummy_value] # AMP expects dummy element for type def.
        )
      )
    when :object # AMP 'struct' (object with fixed keys).
      # `fields_or_element_type` is hash defining object's fields.
      entries = (fields_or_element_type || {}).map do |field_name, (field_type_symbol, subfields_hash)|
        PluginAdapter::Api::Label::Parameter::Value::Hash::Entry.new(
          key: PluginAdapter::Api::Label::Parameter::Value.new(string: field_name), # Field name as key.
          value: build_value(field_type_symbol, subfields_hash) # Recurse for field value.
        )
      end
      PluginAdapter::Api::Label::Parameter::Value.new(
        struct: PluginAdapter::Api::Label::Parameter::Value::Hash.new(entries: entries)
      )

    else
      raise "#{type} not yet implemented in build_value"
    end
  end

  # Generic helper to create label definition structure.
  def label(name, direction, parameters, channel)
    label = PluginAdapter::Api::Label.new
    label.type    = direction
    label.label   = name
    label.channel = channel
    parameters.each { |param| label.parameters << param }
    label
  end
end
