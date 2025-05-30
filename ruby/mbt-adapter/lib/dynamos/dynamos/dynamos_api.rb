class DynamosApi
  def initialize(api_gateway_url = 'http://api-gateway.api-gateway.svc.cluster.local:8080/api/v1')
    @api_gateway_url = api_gateway_url
  end

  def stimulate_dynamos(request_body, endpoint = 'requestApproval')
    uri = URI.parse("#{@api_gateway_url}/#{endpoint}")
    request_properties = { 'Content-Type' => 'application/json' }

    logger.info("Stimulating SUT at URL: #{uri}")
    logger.info("Request body: #{request_body}")

    begin
      response = Net::HTTP.post(uri, request_body, request_properties)
      # Return the HTTP status code and the raw body string
      { code: response.code.to_i, body_str: response.body }
    rescue StandardError => e # Generic rescue for any HTTP/network related errors
      logger.error("Error during HTTP POST to #{uri}: #{e.class.name} - #{e.message}")
      { code: 599, body_str: nil } # 599 for general client-side/network error
    end
  end

  def parse_response(body_str)
    return nil if body_str.nil? || body_str.strip.empty?
    parsed = JSON.parse(body_str)
    if parsed.key?("jobId") && parsed.key?("responses") && parsed["responses"].is_a?(Array) && parsed["responses"].any? { |r| !r.to_s.strip.empty? }
      return parsed
    else
      return nil
    end
  rescue JSON::ParserError
    logger.error("Failed to parse HTTP response body as JSON: #{body_str}")
    return nil
  end

  # def request_body
  #   {
  #     type: 'sqlDataRequest',
  #     user: {
  #       id: '12324',
  #       userName: 'jorrit.stutterheim@cloudnation.nl'
  #     },
  #     dataProviders: %w[VU UVA RUG],
  #     data_request: {
  #       type: 'sqlDataRequest',
  #       query: 'SELECT * FROM Personen p JOIN Aanstellingen s LIMIT 1000',
  #       # // "query" : "SELECT p.Geslacht, s.Salschal FROM Personen p JOIN Aanstellingen s ON p.Unieknr = s.Unieknr",
  #       # // "query" : "SELECT DISTINCT p.Unieknr, p.Geslacht, p.Gebdat, s.Aanst_22, s.Functcat, s.Salschal as Salary FROM Personen p JOIN Aanstellingen s ON p.Unieknr = s.Unieknr LIMIT 4",
  #       algorithm: 'average',
  #       # // "algorithmColumns" : {
  #       # //     "Geslacht" : "Aanst_22, Gebdat"
  #       # // },
  #       options: {
  #         graph: false,
  #         aggregate: false
  #       },
  #       requestMetadata: {}
  #     }
  #   }.to_json
  # end

end
