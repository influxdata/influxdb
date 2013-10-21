require "#{File.dirname(__FILE__)}/spec_helper.rb"

describe "http api" do
  it "writes points and answers queries" do
    response = post_with_body("/db", {}, {name: "test", apiKey: "key1"}.to_json)
    response[:code].should == 201

    data = [{
      points: [
        ["1", 1, 1.0, true],
        ["2", 2, 2.0, false],
        ["3", 3, 3.0, true]
      ],
      name: "foo",
      integer_columns: [1],
      columns: ["column_one", "column_two", "column_three", "column_four"]
    }]
    response = post_with_body("/db/test/series", {api_key: "key1"}, data.to_json)
    response[:code].should == 200
    query = "select * from foo;"
    response = get("/db/test/series", {api_key: "key1", q: query})
    response[:code].should == 200
    response[:parsed_body].length.should == 1
    response[:parsed_body][0]["points"].length.should == 3
  end
end