require "formula"

class Influxdb < Formula
  homepage "http://influxdb.org"
  url "https://s3.amazonaws.com/influxdb/influxdb-0.0.1.tar.gz"
  sha1 "f2faabc714d621da460573163eaffa6ee02f9b6e"
  depends_on "leveldb"

  def install
    inreplace "config.json" do |s|
      s.gsub! "/tmp/influxdb/development/db", "#{var}/influxdb/data"
      s.gsub! "/tmp/influxdb/development/raft", "#{var}/influxdb/raft"
      s.gsub! "./src/admin/site/", "#{share}/admin/"
    end

    bin.install "influxdb"
    etc.install "config.json" => "influxdb.conf"
    share.install "admin"

    %w[influxdb infludxb/data influxdb/raft].each { |p| (var+p).mkpath }
  end

  plist_options :manual => "influxdb -config=#{HOMEBREW_PREFIX}/etc/influxdb.conf"

  def plist; <<-EOS.undent
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
      <dict>
        <key>KeepAlive</key>
        <dict>
          <key>SuccessfulExit</key>
          <false/>
        </dict>
        <key>Label</key>
        <string>#{plist_name}</string>
        <key>ProgramArguments</key>
        <array>
          <string>#{opt_prefix}/bin/influxdb</string>
          <string>-config=#{etc}/influxdb.conf</string>
        </array>
        <key>RunAtLoad</key>
        <true/>
        <key>WorkingDirectory</key>
        <string>#{var}</string>
        <key>StandardErrorPath</key>
        <string>#{var}/log/influxdb.log</string>
        <key>StandardOutPath</key>
        <string>#{var}/log/influxdb.log</string>
      </dict>
    </plist>
    EOS
  end

  test do
    system "curl --silent http://localhost:8086/"
  end
end
