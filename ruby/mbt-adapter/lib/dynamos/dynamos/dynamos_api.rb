# Talks HTTP to DYNAMOS API Gateway.
class DynamosApi
  def initialize(api_gateway_url = 'http://api-gateway.api-gateway.svc.cluster.local:8080/api/v1')
    @api_gateway_url = api_gateway_url
  end

  # POST to /requestApproval (stimulus).
  def stimulate_dynamos(request_body, endpoint = 'requestApproval')
    uri = URI.parse("#{@api_gateway_url}/#{endpoint}")
    log_http_request("Stimulating SUT", uri, request_body)
    http_post(uri, request_body)
  end

  # PUT to orchestrator for archetype switch.
  def switch_archetype(request_body)
    orchestrator_url = 'http://orchestrator.orchestrator.svc.cluster.local:8080/api/v1'
    uri = URI.parse("#{orchestrator_url}/archetypes/agreements")
    log_http_request("Switching archetype", uri, request_body)
    http_put(uri, request_body)
  end

  # Parses HTTP response body (expects jobId + responses).
  def parse_response(body_str)
    return nil if body_str.nil? || body_str.strip.empty?
    parsed = JSON.parse(body_str)
    if parsed.key?("jobId") && parsed.key?("responses") && parsed["responses"].is_a?(Array)
      parsed
    else
      nil
    end
  rescue JSON::ParserError
    logger.error("Failed to parse HTTP response body as JSON: #{body_str}")
    nil
  end

  private

  # Logs HTTP request details.
  def log_http_request(action, uri, body)
    logger.info("#{action} at URL: #{uri}")
    logger.info("Request body: #{body}")
  end

  # POST helper.
  def http_post(uri, body)
    request_properties = { 'Content-Type' => 'application/json' }
    Net::HTTP.post(uri, body, request_properties).yield_self do |response|
      { code: response.code.to_i, body_str: response.body }
    end
  rescue StandardError => e
    log_http_error("HTTP POST", uri, e)
    { code: 0, body_str: nil }
  end

  # PUT helper.
  def http_put(uri, body)
    http = Net::HTTP.new(uri.host, uri.port)
    request = Net::HTTP::Put.new(uri.path, 'Content-Type' => 'application/json')
    request.body = body
    http.request(request).yield_self do |response|
      { code: response.code.to_i, body_str: response.body }
    end
  rescue StandardError => e
    log_http_error("HTTP PUT", uri, e)
    { code: 0, body_str: nil }
  end

  # Logs HTTP errors.
  def log_http_error(method, uri, exception)
    logger.error("Error during #{method} to #{uri}: #{exception.class.name} - #{exception.message}")
  end
end
