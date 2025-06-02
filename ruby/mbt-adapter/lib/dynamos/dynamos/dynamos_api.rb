# Handles HTTP communication with the DYNAMOS API Gateway.
class DynamosApi
  # @param api_gateway_url [String] Base URL of the DYNAMOS API.
  def initialize(api_gateway_url = 'http://api-gateway.api-gateway.svc.cluster.local:8080/api/v1')
    @api_gateway_url = api_gateway_url
  end

  # Sends a POST request to the DYNAMOS API.
  # This is the primary method for stimulating the SUT via HTTP.
  # @param request_body [String] JSON request body.
  # @param endpoint [String] API endpoint (e.g., 'requestApproval').
  # @return [Hash] Contains :code (Integer HTTP status) and :body_str (String or nil).
  def stimulate_dynamos(request_body, endpoint = 'requestApproval')
    uri = URI.parse("#{@api_gateway_url}/#{endpoint}")
    request_properties = { 'Content-Type' => 'application/json' }

    logger.info("Stimulating SUT at URL: #{uri}")
    logger.info("Request body: #{request_body}")

    begin
      response = Net::HTTP.post(uri, request_body, request_properties)
      { code: response.code.to_i, body_str: response.body }
    rescue StandardError => e # Catch network or HTTP errors.
      logger.error("Error during HTTP POST to #{uri}: #{e.class.name} - #{e.message}")
      # Return 599 for client-side/network errors, and a nil body.
      { code: 599, body_str: nil }
    end
  end

  # Parses the HTTP response body (JSON) for a "results" structure.
  # Specifically looks for "jobId" and "responses" keys.
  # @param body_str [String, nil] Raw HTTP response body.
  # @return [Hash, nil] Parsed hash if valid "results" structure, otherwise nil.
  def parse_response(body_str)
    # Return nil if body is empty or not parsable.
    return nil if body_str.nil? || body_str.strip.empty?
    parsed = JSON.parse(body_str)
    # Check for essential "results" keys and that "responses" is a non-empty array.
    if parsed.key?("jobId") && parsed.key?("responses") && parsed["responses"].is_a?(Array) && parsed["responses"].any? { |r| !r.to_s.strip.empty? }
      return parsed
    else
      return nil # Body does not match expected "results" structure.
    end
  rescue JSON::ParserError # Handle invalid JSON.
    logger.error("Failed to parse HTTP response body as JSON: #{body_str}")
    return nil
  end

end

