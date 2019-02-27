// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {IndexList} from 'src/clockface'

// Types
import {LogEvent} from '@influxdata/influx'

interface Props {
  log: LogEvent
}

class RunLogRow extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {log} = this.props

    return (
      <IndexList.Row>
        <IndexList.Cell>{this.dateTimeString(log.time)}</IndexList.Cell>
        <IndexList.Cell>{log.message}</IndexList.Cell>
      </IndexList.Row>
    )
  }

  private dateTimeString(dt: Date): string {
    const date = dt.toDateString()
    const time = dt.toLocaleTimeString()
    const formatted = `${date} ${time}`

    return formatted
  }
}

export default RunLogRow
