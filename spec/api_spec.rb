describe "administration" do
  it "creates a database"
  it "adds a write key to a database"
  it "adds a read key to a database"
  it "deletes a database"
end

describe "POSTing" do
  before :all do
    @db = "some_db"
  end

  it "posts points with given timestamps to a series" do
    t = Time.now.to_i
    response = post("/db/#{@db}/points", [
      {
        "series" => "users.events",
        "columns" => ["timestamp", "email", "type"],
        "points" => [
          [t, "paul@errplane.com", "click"],
          [t, "todd@errplane.com", "click"]
        ]
      },
      {
        "series" => "cpu.idle",
        "columns" => ["timestamp", "value"],
        "points" => [
          [t, 97.3]
        ]
      }
    ])
  end

  it "posts points to a series and assigns a timestamp of now on the server" do
    response = post("/db/#{@db}/points", [
      {
        "series" => "users.events",
        "columns" => ["email", "type"],
        "points" => [
          ["paul@errplane.com", "click"],
          ["todd@errplane.com", "click"]
        ]
      }
    ])
  end
end

describe "GETing" do
  before :all do
    @db = "some_db"
  end

  it "returns a time series with a start and end time with a default order of newest first" do
    response = get("/db/#{@db}/series?q=select value from cpu.idle where time>now()-1d")
    response_body_without_ids(response).should == [
      {
        "series" => "cpu.idle",
        "columns" => ["value", "time"],
        "datapoints" => [
          [6.0, 1311836012],
          [5.0, 1311836011],
          [3.0, 1311836010],
          [2.0, 1311836009],
          [1.0, 1311836008]
        ]
      }
    ]
  end

  it "returns multiple time series with a start and end time" do
    response = get("/db/#{@db}/series?q=select value from cpu.* where time>now()-7d and time<now()-6d")
    response_body_without_ids(response).should == [
      {
        "series" => "cpu.idle",
        "columns" => ["value", "time"],
        "datapoints" => [
          [2.0, 1311836009],
          [1.0, 1311836008]
        ]
      },
      {
        "series" => "cpu.sys",
        "columns" => ["value", "time"],
        "datapoints" => [
          [2.0, 1311836009],
          [1.0, 1311836008]
        ]
      }
    ]
  end

  it "splits unique column values into multiple points for each group by period" do
    write_points(@db, "users.events", [
      {email: "paul@errplane.com", type: "click", target: "/foo/bar/something"},
      {email: "todd@errplane.com", type: "click", target: "/asdf"},
      {email: "paul@errplane.com", type: "click", target: "/jkl", time: 2.days.ago.to_i}
    ])
    response = get("/db/#{@db}/series?q=select count(*) from users.events group_by user_email,time(1h) where time>now()-7d")
    response_body_without_ids(response).should == [
      {
        "series" => "users.events",
        "columns" => ["count", "time", "email"],
        "datapoints" => [
          [1, 1311836012, "paul@errplane.com"],
          [1, 1311836012, "todd@errplane.com"],
          [1, 1311836008, "paul@errplane.com"]
        ]
      }
    ]
  end

  it "returns the top n time series by a given function" do
    response = get("/db/#{@db}/series?q=select top(10, count(*)) from users.events group_by user_email,time(1h) where time>now()-7d")
    # response has 10 points per grouped by time interval
  end

  it "returns multiple time series by taking a regex in the from clause with a limit on the number of points with newest first (last point from every series)" do
    response = get("/db/#{@db}/series?q=select value from .* last 1")
    response_body_without_ids(response).should == [
      {
        "series" => "users.events",
        "columns" => ["value", "time"],
        "datapoints" => [
          [1, 1311836012]
        ]
      },
      {
        "series" => "cpu.idle",
        "columns" => ["value", "time"],
        "datapoints" => [
          [6.0, 1311836012]
        ]
      }
    ]
  end

  it "has a default where of time>now()-1h"
  it "has a default limit of 1000"

  it "can merge two time series into one" do
    response = get("/db/#{@db}/series?q=select count(*) from merge(newsletter.signups,user.signups) group_by time(1h) where time>now()-1d")
  end

  it "returns a time series that is the result of a diff of one series against another" do
    response = get("/db/#{@db}/series?q=select diff(t1.value, t2.value) from inner_join(memory.total, t1, memory.used, t2) group_by time(1m) where time>now()-6h")
  end

  it "returns a time series with the function applied to a time series and a scalar"

  it "returns a time series with counted unique values" do
    response = get("/db/#{@db}/series?q=select count(distinct(email)) from user.events where time>now()-1d group_by time(15m)")
  end

  it "returns a time series with calculated percentiles" do
    response = get("/db/#{@db}/series?q=select percentile(95, value) from response_times group_by time(10m) where time>now()-6h")
  end

  it "returns a single time series with the unique dimension values for dimension a for a given dimension=<some value>" do
    series_name = "events"
    write_points(@db, events, {email: "paul@errplane.com", type: "login"})
    response = get("/db/#{@db}/series?q=select count(*) from events where type='login'")
  end

  it "runs a continuous query with server sent events"

  # continuous queries redirected into a time series run forever
  it "can redirect a continuous query into a new time series" do
    response = get("/db/#{@db}/series?q=insert into user.events.count.per_day select count(*) from user.events where time<forever group_by time(1d)")
  end

  it "can redirect a continuous query of multiple series into a many new time series" do
    response = get("/db/#{@db}/series?q=insert into :series_name.percentiles.95 select percentile(95,value) from stats.* where time<forever group_by time(1d)")
  end

  it "has an index of the running continuous queries"
  it "can stop a continuous query"

  it "can match against a regex in the where clause" do
    response = get("/db/#{@db}/series?q=select email from users.events where email ~= /gmail\.com/i and time>now()-2d group_by time(10m)")
  end
end

describe "Indexes" do
  before :all do
    @db = "test_index_db"
  end

  it "can index a single column and store a specific column in the index for quick retrieval" do
    response = post("/db/#{@db}/indexes", {series: "cpu.idle", index_columns: ["host"], store_columns: ["value"]}.to_json)
  end

  it "can index on multiple columns" do
    response = post("/db/#{@db}/indexes", {series: "events", index_columns: ["type", "region"], store_columns: ["email"]}.to_json)
  end
end

describe "DELETEing" do
  it "deletes a range of data from start to end time from a time series"
  it "deletes a range of data from start to end time for any time series matching a regex"
end
