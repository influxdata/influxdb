// prettier-ignore
export const RESPONSE_METADATA = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true
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
    'partition',
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
#partition,false,false,false,false,false,false
#default,_result,,,,,
,result,table,_start,_stop,_measurement,_value
,,0,2018-05-24T21:48:17.127227579Z,2018-05-24T22:48:17.127227579Z,disk,disk
,,0,2018-05-24T21:48:17.127227579Z,2018-05-24T22:48:17.127227579Z,diskio,diskio

`

// prettier-ignore
export const LARGE_RESPONSE = `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,cpu,host
,,0,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu-total,WattsInfluxDB
,,1,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu-total,WattsInfluxDB
,,2,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,70.76923076923077,usage_idle,cpu,cpu-total,WattsInfluxDB
,,3,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu-total,WattsInfluxDB
,,4,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu-total,WattsInfluxDB
,,5,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu-total,WattsInfluxDB
,,6,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu-total,WattsInfluxDB
,,7,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu-total,WattsInfluxDB
,,8,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,11.794871794871796,usage_system,cpu,cpu-total,WattsInfluxDB
,,9,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,17.435897435897434,usage_user,cpu,cpu-total,WattsInfluxDB
,,10,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu0,WattsInfluxDB
,,11,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu0,WattsInfluxDB
,,12,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,47.82608695652174,usage_idle,cpu,cpu0,WattsInfluxDB
,,13,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu0,WattsInfluxDB
,,14,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu0,WattsInfluxDB
,,15,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu0,WattsInfluxDB
,,16,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu0,WattsInfluxDB
,,17,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu0,WattsInfluxDB
,,18,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,26.08695652173913,usage_system,cpu,cpu0,WattsInfluxDB
,,19,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,26.08695652173913,usage_user,cpu,cpu0,WattsInfluxDB
,,20,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu1,WattsInfluxDB
,,21,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu1,WattsInfluxDB
,,22,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,92,usage_idle,cpu,cpu1,WattsInfluxDB
,,23,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu1,WattsInfluxDB
,,24,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu1,WattsInfluxDB
,,25,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu1,WattsInfluxDB
,,26,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu1,WattsInfluxDB
,,27,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu1,WattsInfluxDB
,,28,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_system,cpu,cpu1,WattsInfluxDB
,,29,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_user,cpu,cpu1,WattsInfluxDB
,,30,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu2,WattsInfluxDB
,,31,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu2,WattsInfluxDB
,,32,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,52,usage_idle,cpu,cpu2,WattsInfluxDB
,,33,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu2,WattsInfluxDB
,,34,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu2,WattsInfluxDB
,,35,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu2,WattsInfluxDB
,,36,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu2,WattsInfluxDB
,,37,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu2,WattsInfluxDB
,,38,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,16,usage_system,cpu,cpu2,WattsInfluxDB
,,39,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,32,usage_user,cpu,cpu2,WattsInfluxDB
,,40,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu3,WattsInfluxDB
,,41,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu3,WattsInfluxDB
,,42,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,91.66666666666667,usage_idle,cpu,cpu3,WattsInfluxDB
,,43,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu3,WattsInfluxDB
,,44,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu3,WattsInfluxDB
,,45,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu3,WattsInfluxDB
,,46,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu3,WattsInfluxDB
,,47,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu3,WattsInfluxDB
,,48,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_system,cpu,cpu3,WattsInfluxDB
,,49,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_user,cpu,cpu3,WattsInfluxDB
,,50,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu4,WattsInfluxDB
,,51,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu4,WattsInfluxDB
,,52,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,45.833333333333336,usage_idle,cpu,cpu4,WattsInfluxDB
,,53,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu4,WattsInfluxDB
,,54,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu4,WattsInfluxDB
,,55,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu4,WattsInfluxDB
,,56,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu4,WattsInfluxDB
,,57,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu4,WattsInfluxDB
,,58,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,16.666666666666668,usage_system,cpu,cpu4,WattsInfluxDB
,,59,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,37.5,usage_user,cpu,cpu4,WattsInfluxDB
,,60,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu5,WattsInfluxDB
,,61,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu5,WattsInfluxDB
,,62,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,92,usage_idle,cpu,cpu5,WattsInfluxDB
,,63,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu5,WattsInfluxDB
,,64,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu5,WattsInfluxDB
,,65,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu5,WattsInfluxDB
,,66,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu5,WattsInfluxDB
,,67,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu5,WattsInfluxDB
,,68,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_system,cpu,cpu5,WattsInfluxDB
,,69,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4,usage_user,cpu,cpu5,WattsInfluxDB
,,70,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu6,WattsInfluxDB
,,71,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu6,WattsInfluxDB
,,72,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,52,usage_idle,cpu,cpu6,WattsInfluxDB
,,73,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu6,WattsInfluxDB
,,74,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu6,WattsInfluxDB
,,75,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu6,WattsInfluxDB
,,76,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu6,WattsInfluxDB
,,77,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu6,WattsInfluxDB
,,78,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,20,usage_system,cpu,cpu6,WattsInfluxDB
,,79,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,28,usage_user,cpu,cpu6,WattsInfluxDB
,,80,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest,cpu,cpu7,WattsInfluxDB
,,81,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_guest_nice,cpu,cpu7,WattsInfluxDB
,,82,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,91.66666666666667,usage_idle,cpu,cpu7,WattsInfluxDB
,,83,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_iowait,cpu,cpu7,WattsInfluxDB
,,84,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_irq,cpu,cpu7,WattsInfluxDB
,,85,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_nice,cpu,cpu7,WattsInfluxDB
,,86,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_softirq,cpu,cpu7,WattsInfluxDB
,,87,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,usage_steal,cpu,cpu7,WattsInfluxDB
,,88,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_system,cpu,cpu7,WattsInfluxDB
,,89,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,4.166666666666667,usage_user,cpu,cpu7,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,90,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,182180679680,free,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,91,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,9223372036852008920,inodes_free,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,92,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,9223372036854775807,inodes_total,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,93,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,2766887,inodes_used,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,94,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,499963170816,total,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A
,,95,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,314933657600,used,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,96,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T21:05:08.947Z,63.352358598865635,used_percent,disk,/Users/watts/Downloads/TablePlus.app,nullfs,WattsInfluxDB,ro,/private/var/folders/f4/zd7n1rqj7xj6w7c0njkmmjlh0000gn/T/AppTranslocation/F4D8D166-F848-4862-94F6-B51C00E2EB7A

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,97,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,free,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,98,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,inodes_free,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,99,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,716,inodes_total,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,100,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,716,inodes_used,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,101,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,211968,total,disk,devfs,devfs,WattsInfluxDB,rw,/dev
,,102,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,211968,used,disk,devfs,devfs,WattsInfluxDB,rw,/dev

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,103,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,100,used_percent,disk,devfs,devfs,WattsInfluxDB,rw,/dev

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,104,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,258453504,free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,105,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,0,inodes_free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,106,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,0,inodes_total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,107,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,0,inodes_used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,108,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,313827328,total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm
,,109,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,55373824,used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,110,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-18T03:13:34.143Z,17.644678796105353,used_percent,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/bless.9mnm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,111,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,274092032,free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,112,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,0,inodes_free,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,113,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,0,inodes_total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,114,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,0,inodes_used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,115,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,313827328,total,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB
,,116,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,39735296,used,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,117,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-12T05:09:50.004Z,12.66151557075361,used_percent,disk,disk0s1,msdos,WattsInfluxDB,rw,/Volumes/firmwaresyncd.ushpRB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,118,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,220499697664,free,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,119,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036852024886,inodes_free,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,120,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036854775807,inodes_total,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,121,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,2750921,inodes_used,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,122,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,499963170816,total,disk,disk1s1,apfs,WattsInfluxDB,rw,/
,,123,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,275540910080,used,disk,disk1s1,apfs,WattsInfluxDB,rw,/

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,124,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,55.54805509435289,used_percent,disk,disk1s1,apfs,WattsInfluxDB,rw,/

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,125,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,171371986944,free,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,126,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,9223372036854775741,inodes_free,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,127,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,9223372036854775807,inodes_total,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,128,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,66,inodes_used,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,129,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,499963170816,total,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1
,,130,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,21532672,used,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,131,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-20T04:05:50.608Z,0.012563294136349525,used_percent,disk,disk1s2,apfs,WattsInfluxDB,rw,/Volumes/Preboot 1

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,132,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,167696769024,free,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,133,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,9223372036854775793,inodes_free,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,134,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,9223372036854775807,inodes_total,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,135,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,14,inodes_used,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,136,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,499963170816,total,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery
,,137,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,517763072,used,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,138,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-19T19:33:36.939Z,0.30779925226942506,used_percent,disk,disk1s3,apfs,WattsInfluxDB,rw,/Volumes/Recovery

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,139,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,220499697664,free,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,140,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036854775804,inodes_free,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,141,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,9223372036854775807,inodes_total,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,142,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3,inodes_used,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,143,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,499963170816,total,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm
,,144,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3221266432,used,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,145,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,1.4398589980229728,used_percent,disk,disk1s4,apfs,WattsInfluxDB,rw,/private/var/vm

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,146,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,free,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,147,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,3390710,inodes_free,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,148,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,71,inodes_total,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,149,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,inodes_used,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,150,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,6944174080,total,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,151,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,used,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,152,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-07T04:06:42.333Z,0,used_percent,disk,disk2,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,153,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,389857280,free,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,154,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,4294966864,inodes_free,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,155,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,4294967279,inodes_total,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,156,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,415,inodes_used,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,157,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,1698652160,total,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429
,,158,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,1308794880,used,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,159,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-14T16:10:41.251Z,77.04902220829013,used_percent,disk,disk2s1,hfs,WattsInfluxDB,ro,/Volumes/FF95FBB6-192D-47B9-BBBC-833F6368D429

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,160,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,0,free,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,161,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,4294966918,inodes_free,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,162,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,4294967279,inodes_total,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,163,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,361,inodes_used,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,164,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,185028608,total,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m
,,165,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,185028608,used,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,166,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-17T22:33:07.478Z,100,used_percent,disk,disk2s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.q62GM7vGxK/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,167,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,free,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,168,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,3426691,inodes_free,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,169,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,41,inodes_total,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,170,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,inodes_used,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,171,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,7017863168,total,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok
,,172,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,173,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used_percent,disk,disk3,udf,WattsInfluxDB,ro,/Volumes/Thor Ragnarok

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,174,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,389857280,free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,175,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,4294966864,inodes_free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,176,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,4294967279,inodes_total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,177,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,415,inodes_used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,178,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,1698652160,total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57
,,179,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,1308794880,used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,180,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-11T17:15:35.255Z,77.04902220829013,used_percent,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/0FD2794B-226F-4DEB-A19C-75E005A6AC57

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,181,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,105676800,free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,182,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,4294967273,inodes_free,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,183,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,4294967279,inodes_total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,184,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,6,inodes_used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,185,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,609431552,total,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta
,,186,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,503754752,used,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,187,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:46:03.067Z,82.65977538360207,used_percent,disk,disk3s1,hfs,WattsInfluxDB,ro,/Volumes/RecoveryHDMeta

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,188,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,free,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,189,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,3390710,inodes_free,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,190,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,71,inodes_total,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,191,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,inodes_used,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,192,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,6944174080,total,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]
,,193,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,194,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-04T16:44:35.069Z,0,used_percent,disk,disk4,udf,WattsInfluxDB,ro,/Volumes/Jumanji.Benvenuti.Nella.Giungla[Kasdan.2017.dvd9.kx]

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,195,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,389865472,free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,196,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,4294966864,inodes_free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,197,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,4294967279,inodes_total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,198,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,415,inodes_used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,199,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,1698652160,total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B
,,200,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,1308786688,used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,201,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-26T16:25:42.052Z,77.04853994357502,used_percent,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/55713F97-1C88-4076-B6BB-8897592CB08B

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,202,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,444579840,free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,203,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,4294967132,inodes_free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,204,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,4294967279,inodes_total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,205,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,147,inodes_used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,206,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,524247040,total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C
,,207,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,79667200,used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,208,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-22T19:11:41.599Z,15.196499726541134,used_percent,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/A81441DB-3D0C-48E8-8BED-F53FCD1D6D3C

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,209,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,7413760,free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,210,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,4294966699,inodes_free,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,211,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,4294967279,inodes_total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,212,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,580,inodes_used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,213,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,40693760,total,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick
,,214,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,33280000,used,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,215,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-04-27T17:08:29.908Z,81.78158027176649,used_percent,disk,disk4s1,hfs,WattsInfluxDB,ro,/Volumes/Tunnelblick

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,216,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,0,free,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,217,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,4294966918,inodes_free,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,218,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,4294967279,inodes_total,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,219,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,361,inodes_used,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,220,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,185032704,total,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m
,,221,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,185032704,used,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true,true,true,true,true
#default,_result,,,,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,device,fstype,host,mode,path
,,222,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-01T19:26:01.539Z,100,used_percent,disk,disk5s2,hfs,WattsInfluxDB,ro,/private/tmp/KSInstallAction.1EQur33ekx/m

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,223,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,6318931968,active,mem,WattsInfluxDB
,,224,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,5277085696,available,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,225,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,30.716681480407715,available_percent,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,226,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,buffered,mem,WattsInfluxDB
,,227,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,cached,mem,WattsInfluxDB
,,228,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,1897549824,free,mem,WattsInfluxDB
,,229,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3379535872,inactive,mem,WattsInfluxDB
,,230,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,0,slab,mem,WattsInfluxDB
,,231,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,17179869184,total,mem,WattsInfluxDB
,,232,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,11902783488,used,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,233,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,69.28331851959229,used_percent,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,234,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.584Z,3103551488,wired,mem,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,235,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,8.66,load1,system,WattsInfluxDB
,,236,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,3.78,load15,system,WattsInfluxDB
,,237,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,5.35,load5,system,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,238,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,8,n_cpus,system,WattsInfluxDB
,,239,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.589Z,11,n_users,system,WattsInfluxDB
,,240,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.34Z,90708,uptime,system,WattsInfluxDB

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,string
#partition,false,false,false,false,false,false,true,true,true
#default,_result,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,host
,,241,1677-09-21T00:12:43.145224192Z,2018-05-22T22:39:17.042276772Z,2018-05-22T22:39:12.34Z,"1 day,  1:11",uptime_format,system,WattsInfluxDB


`
