export const usageProps = {
  history: {
    billingStats:
      '#group,false,false,true,true,false,false\r\n#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,double\r\n#default,writes_mb,,,,,\r\n,result,table,_start,_stop,_value,writes_mb\r\n,,0,2020-05-01T00:00:00Z,2020-05-06T22:26:05.152150425Z,2486018149,2486.02\r\n\r\n#group,false,false,true,true,false,false\r\n#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,long,double\r\n#default,execution_sec,,,,,\r\n,result,table,_start,_stop,_value,execution_sec\r\n,,0,2020-05-01T00:00:00Z,2020-05-06T22:26:05.15215042...',
    executionSec:
      '#group,false,false,false,false,false\r\n#datatype,string,long,dateTime:RFC3339,long,double\r\n#default,execution_sec,,,,\r\n,result,table,_time,_value,execution_sec\r\n,,0,2020-05-05T23:00:00Z,0,0\r\n,,0,2020-05-06T00:00:00Z,84275,0.08\r\n,,0,2020-05-06T01:00:00Z,352690,0.35\r\n,,0,2020-05-06T02:00:00Z,225947,0.23\r\n,,0,2020-05-06T03:00:00Z,255300,0.26\r\n,,0,2020-05-06T04:00:00Z,71296,0.07\r\n,,0,2020-05-06T05:00:00Z,291288,0.29\r\n,,0,2020-05-06T06:00:00Z,71943,0.07\r\n,,0,2020-05-06T07:00:00Z,295893,0.3\r\n,,0,2020-0...',
    rateLimits: '\r\n',
    storageGB:
      '#group,false,false,true,false,false\r\n#datatype,string,long,dateTime:RFC3339,double,double\r\n#default,storage_gb,,,,\r\n,result,table,_time,_value,storage_gb\r\n,,0,2020-05-05T23:00:00Z,243056751.28078824,0.24\r\n,,1,2020-05-06T00:00:00Z,242910336.175,0.24\r\n,,2,2020-05-06T01:00:00Z,257373287.171777,0.26\r\n,,3,2020-05-06T02:00:00Z,280993234.13611114,0.28\r\n,,4,2020-05-06T03:00:00Z,281176978.5777778,0.28\r\n,,5,2020-05-06T04:00:00Z,280965187.3833334,0.28\r\n,,6,2020-05-06T05:00:00Z,281290973.15555555,0.28\r\n,,7,...',
    writeMB:
      '#group,false,false,false,false,false\r\n#datatype,string,long,dateTime:RFC3339,long,double\r\n#default,write_mb,,,,\r\n,result,table,_time,_value,write_mb\r\n,,0,2020-05-05T23:00:00Z,9942887,9.94\r\n,,0,2020-05-06T00:00:00Z,17632355,17.63\r\n,,0,2020-05-06T01:00:00Z,17632175,17.63\r\n,,0,2020-05-06T02:00:00Z,17633074,17.63\r\n,,0,2020-05-06T03:00:00Z,17632874,17.63\r\n,,0,2020-05-06T04:00:00Z,17632858,17.63\r\n,,0,2020-05-06T05:00:00Z,17632440,17.63\r\n,,0,2020-05-06T06:00:00Z,17632570,17.63\r\n,,0,2020-05-06T07:00:00Z...',
  },
  limitStatuses: {
    cardinality: {status: 'ok'},
    read: {status: 'ok'},
    write: {status: 'ok'},
  },
  selectedRange: 'h24',
  accountType: 'free',
  billingStart: {
    date: '2020-05-01',
    time: '2020-05-01T00:00:00Z',
  },
}
