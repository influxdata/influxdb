require "influxdb"
require "colorize"

module Log
  def self.info(msg)
    print Time.now.strftime("%r") + " | "
    puts msg.to_s.colorize(:yellow)
  end

  def self.success(msg)
    print Time.now.strftime("%r") + " | "
    puts msg.to_s.colorize(:green)
  end

  def self.failure(msg)
    print Time.now.strftime("%r") + " | "
    puts msg.to_s.colorize(:red)
  end
end

Log.info "Starting burn-in suite"

influxdb = InfluxDB::Client.new

Log.success "Connected to server #{influxdb.host}:#{influxdb.port}"

result = influxdb.query("select * from foo;")

Log.info(result)
