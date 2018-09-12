// prettier-ignore
export const RESPONSE_METADATA = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-23T17:42:29.536834648Z,2018-05-23T17:43:29.536834648Z,2018-05-23T17:42:29.654Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB
`

export const RESPONSE_NO_METADATA = `,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-05-23T17:42:29.536834648Z,2018-05-23T17:43:29.536834648Z,2018-05-23T17:42:29.654Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB

`

export const RESPONSE_NO_MEASUREMENT = `,result,table,_start,_stop,_time,_value,_field,cpu,host
,,0,2018-05-23T17:42:29.536834648Z,2018-05-23T17:43:29.536834648Z,2018-05-23T17:42:29.654Z,0,usage_guest,cpu-total,WattsInfluxDB`

export const EXPECTED_COLUMNS = [
  '',
  'result',
  'table',
  '_start',
  '_stop',
  '_time',
  '_value',
  '_field',
  '_measurement',
  'cpu',
  'host',
]

export const EXPECTED_METADATA = [
  [
    'datatype',
    'string',
    'long',
    'dateTime:RFC3339',
    'dateTime:RFC3339',
    'dateTime:RFC3339',
    'double',
    'string',
    'string',
    'string',
    'string',
  ],
  [
    'group',
    'false',
    'false',
    'false',
    'false',
    'false',
    'false',
    'true',
    'true',
    'true',
    'true',
  ],
  ['default', '_result', '', '', '', '', '', '', '', '', ''],
]

export const MEASUREMENTS_RESPONSE = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,string,string
#group,false,false,false,false,false,false
#default,_result,,,,,
,result,table,_start,_stop,_measurement,_value
,,0,2018-05-24T21:48:17.127227579Z,2018-05-24T22:48:17.127227579Z,disk,disk
,,0,2018-05-24T21:48:17.127227579Z,2018-05-24T22:48:17.127227579Z,diskio,diskio

`

/* 
From the following request:

    from(db: "telegraf")
      |> range(start: -24h)
      |> group(none: true)
      |> keys(except:["_time","_value","_start","_stop"])
      |> map(fn: (r) => r._value)
*/
export const TAGS_RESPONSE = `#datatype,string,long,string
#group,false,false,false
#default,_result,,
,result,table,_value
,,0,_field
,,0,_measurement
,,0,cpu
,,0,device
,,0,fstype
,,0,host
,,0,mode
,,0,name
,,0,path
`

// prettier-ignore

export const SIMPLE = `
#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,Results,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,2018-09-10T22:34:26.854028Z,2018-09-10T22:35:26.854028Z,2018-09-10T22:34:29Z,0,usage_guest,cpu,cpu1,bertrand.local
,,0,2018-09-10T22:34:26.854028Z,2018-09-10T22:35:26.854028Z,2018-09-10T22:34:39Z,10,usage_guest,cpu,cpu1,bertrand.local


`
export const MULTI_SCHEMA_RESPONSE = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB
,,1,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu-total,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,2,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,182180679680,free,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,3,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,9223372036852008920,inodes_free,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A


`
export const MULTI_VALUE_ROW = `
#datatype,string,long,dateTime:RFC3339,double,double,string
#group,false,false,false,false,false,true
#default,0,,,,,
,result,table,_time,mean_usage_idle,mean_usage_user,_measurement
,,0,2018-09-10T16:54:37Z,85,10,cpu
,,0,2018-09-10T16:54:38Z,87,7,cpu
,,0,2018-09-10T16:54:39Z,89,5,cpu
,,1,2018-09-10T16:54:37Z,8,1,mem
,,1,2018-09-10T16:54:38Z,9,2,mem
,,1,2018-09-10T16:54:39Z,10,3,mem


`

export const MIXED_DATATYPES = `
#datatype,string,long,dateTime:RFC3339,double,string,string
#group,false,false,false,false,false,true
#default,0,,,,,
,result,table,_time,mean_usage_idle,my_fun_col,_measurement
,,0,2018-09-10T16:54:37Z,85,foo,cpu
,,0,2018-09-10T16:54:39Z,89,foo,cpu
,,1,2018-09-10T16:54:37Z,8,bar,mem
,,1,2018-09-10T16:54:39Z,10,bar,mem


`

export const MISMATCHED = `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#group,false,false,true,true,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,0,2018-06-04T17:12:21.025984999Z,2018-06-04T17:13:00Z,2018-06-04T17:12:25Z,1,active,mem,bertrand.local
,,0,2018-06-04T17:12:21.025984999Z,2018-06-04T17:13:00Z,2018-06-04T17:12:35Z,2,active,mem,bertrand.local
,,1,2018-06-04T17:12:21.025984999Z,2018-06-04T17:13:00Z,2018-06-05T17:12:25Z,10,available,mem,bertrand.local
,,1,2018-06-04T17:12:21.025984999Z,2018-06-04T17:13:00Z,2018-06-05T17:12:35Z,11,available,mem,bertrand.local
`

export const TRUNCATED_RESPONSE = `
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB
,,1,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu-total,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#group,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,`
