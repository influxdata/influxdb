export interface ClientLibrary {
  id: string
  name: string
  url: string
  logoUrl: string
}

export const clientCSharpLibrary = {
  id: 'csharp',
  name: 'C#',
  url: 'https://github.com/influxdata/influxdb-client-csharp',
  logoUrl: 'assets/images/client-lib-csharp.svg',
  installingPackageManagerCodeSnippet: `Install-Package InfluxDB.Client`,
  installingPackageDotNetCLICodeSnippet: `dotnet add package InfluxDB.Client`,
  packageReferenceCodeSnippet: `<PackageReference Include="InfluxDB.Client" />`,
  initializeClientCodeSnippet: `  using InfluxDB.Client;
  namespace Examples
  {  
    public class Examples  
    {    
      public static void Main(string[] args)    
      {      
        var client = InfluxDBClientFactory.Create("basepath", "token".ToCharArray());    
      }  
    }
  }`,
  executeQueryCodeSnippet: `  const string query = "from(bucket: \\"my_bucket\\") |> range(start: -1h)";
  var tables = await client.GetQueryApi().QueryAsync(query, "someorgid");
  `,
  writingDataLineProtocolCodeSnippet: `  const string data = "mem,host=host1 used_percent=23.43234543 1556896326";
  using (var writeApi = client.GetWriteApi())
  {  
    writeApi.WriteRecord("bucketID", "orgID", WritePrecision.Ns, data);
  }`,
  writingDataDataPointCodeSnippet: `  var point = PointData  
    .Measurement("mem")  
    .Tag("host", "host1")  
    .Field("used_percent", 23.43234543)  
    .Timestamp(1556896326L, WritePrecision.Ns);
    
  using (var writeApi = client.GetWriteApi())
  {  
    writeApi.WritePoint("bucketID", "orgID", point);
  }`,
  writingDataPocoCodeSnippet: `  var mem = new Mem { Host = "host1", UsedPercent = 23.43234543, Time = DateTime.UtcNow };
  
  using (var writeApi = client.GetWriteApi())
  {  
    writeApi.WriteMeasurement("bucketID", "orgID", WritePrecision.Ns, mem);
  }`,
  pocoClassCodeSnippet: `  [Measurement("mem")]
  private class Mem
  {  
    [Column("host", IsTag = true)] public string Host { get; set; }  
    [Column("used_percent")] public double? UsedPercent { get; set; }  
    [Column(IsTimestamp = true)] public DateTime Time { get; set; }
  }`,
}

export const clientGoLibrary = {
  id: 'go',
  name: 'GO',
  url: 'https://github.com/influxdata/influxdb-client-go',
  logoUrl: 'assets/images/client-lib-go.svg',
  initializeClientCodeSnippet: `  influx, err := influxdb.New(myHTTPInfluxAddress, myToken, influxdb.WithHTTPClient(myHTTPClient))
	  if err != nil {
		  panic(err) // error handling here; normally we wouldn't use fmt but it works for the example
    }
    // Add your app code here
    influx.Close() // closes the client.  After this the client is useless.`,
  writeDataCodeSnippet: `  // we use client.NewRowMetric for the example because it's easy, but if you need extra performance
    // it is fine to manually build the []client.Metric{}.
    myMetrics := []influxdb.Metric{
      influxdb.NewRowMetric(
        map[string]interface{}{"memory": 1000, "cpu": 0.93},
        "system-metrics",
        map[string]string{"hostname": "hal9000"},
        time.Date(2018, 3, 4, 5, 6, 7, 8, time.UTC)),
      influxdb.NewRowMetric(
        map[string]interface{}{"memory": 1000, "cpu": 0.93},
        "system-metrics",
        map[string]string{"hostname": "hal9000"},
        time.Date(2018, 3, 4, 5, 6, 7, 9, time.UTC)),
    }

    // The actual write..., this method can be called concurrently.
    if _, err := influx.Write(context.Background(), "my-awesome-bucket", "my-very-awesome-org", myMetrics...)
    if err != nil {
      log.Fatal(err) // as above use your own error handling here.
    }`,
}

export const clientJavaLibrary = {
  id: 'java',
  name: 'Java',
  url: 'https://github.com/influxdata/influxdb-client-java',
  logoUrl: 'assets/images/client-lib-java.svg',
}

export const clientJSLibrary = {
  id: 'javascript-node',
  name: 'JavaScript/Node.js',
  url: 'https://github.com/influxdata/influxdb-client-js',
  logoUrl: 'assets/images/client-lib-node.svg',
}

export const clientPythonLibrary = {
  id: 'python',
  name: 'Python',
  url: 'https://github.com/influxdata/influxdb-client-python',
  logoUrl: 'assets/images/client-lib-python.svg',
}

export const clientLibraries: ClientLibrary[] = [
  clientCSharpLibrary,
  clientGoLibrary,
  clientJavaLibrary,
  clientJSLibrary,
  clientPythonLibrary,
]
