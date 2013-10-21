require "rubygems"
require "net/http"
require "yajl"
require "yajl/json_gem"
require "uri"
require 'cgi'

server = "localhost"
port   = "8086"
CONNECTION = Net::HTTP.new(server, port)

# HTTP helper methods to test the APIs. Each of these should return the response code, headers, and body
def get(path, params, connection = CONNECTION)
  request = Net::HTTP::Get.new(to_url(path, params))
  process_request(request, connection)
end

def put(path, params, body)
  request = Net::HTTP::Put.new(to_url(path, params))
  request.body = body
  process_request(request)
end

def post_with_body(path, params, body, connection = CONNECTION)
  request = Net::HTTP::Post.new(to_url(path, params))
  request.body = body
  process_request(request, connection)
end

def delete(path, params, connection = CONNECTION)
  request = Net::HTTP::Delete.new(to_url(path, params))
  process_request(request, connection)
end

def to_url(path, params)
  "#{path}?#{params.to_a.map {|hk| hk[1] = CGI.escape(hk[1]); hk.join("=")}.join("&")}"
end

def process_request(request, connection = CONNECTION)
  response = connection.request(request)
  header = response.read_header
  headers = {}

  header.each_header do |h|
    headers[h] = header[h]
  end

  body = response.read_body
  parsed_body = begin
    parse_json(body)
  rescue
    puts "Cannot parse json from: #{body}"
    nil
  end
  {
    :code    => response.code.to_i,
    :body    => body,
    :headers => headers,
    :parsed_body => parsed_body
  }
end

def to_json(object)
  Yajl::Encoder.encode(object)
end

def parse_json(string)
  Yajl::Parser.parse(string)
end

def puts_response(response)
  json = parse_json(response[:body])
  if json
    puts "\n\n"
    puts json["message"] if json.has_key?("message")
    puts json["backtrace"].join("\n") if json.has_key?("backtrace")
    puts "\n"
    puts response.inspect
  end
end
