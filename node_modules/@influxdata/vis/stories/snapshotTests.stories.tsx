import * as React from 'react'
import {storiesOf} from '@storybook/react'

import {fromFlux, Config, Plot} from '../src'

storiesOf('Snapshot Tests', module).add('with multiple minimum values', () => {
  // https://github.com/influxdata/vis/issues/51

  const {table} = fromFlux(
    `#group,false,false,true,false,false
#datatype,string,long,string,long,dateTime:RFC3339
#default,_result,,,,
,result,table,_field,_value,_time
,,0,event,0,2019-05-01T12:00:00Z
,,0,event,0,2019-05-02T00:00:00Z
,,0,event,0,2019-05-02T12:00:00Z
,,0,event,0,2019-05-03T00:00:00Z
,,0,event,0,2019-05-03T12:00:00Z
,,0,event,1,2019-05-04T00:00:00Z
,,0,event,6,2019-05-04T12:00:00Z
,,0,event,0,2019-05-05T00:00:00Z
,,0,event,0,2019-05-05T12:00:00Z
,,0,event,2,2019-05-06T00:00:00Z
,,0,event,0,2019-05-06T12:00:00Z
,,0,event,10,2019-05-07T00:00:00Z
,,0,event,0,2019-05-07T06:00:00Z`
  )

  const config: Config = {
    width: 600,
    height: 400,
    table,
    layers: [
      {
        type: 'line',
        x: '_time',
        y: '_value',
      },
    ],
  }

  return <Plot config={config} />
})
