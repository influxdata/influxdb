// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import moment from 'moment'

// Components
import {IndexList} from 'src/clockface'

// Types
import {LogEvent} from '@influxdata/influx'
import {DEFAULT_TIME_FORMAT} from 'src/shared/constants'

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
        <IndexList.Cell>
          <span className="run-logs--list-time">
            {this.dateTimeString(log.time)}
          </span>
        </IndexList.Cell>
        <IndexList.Cell>
          <span className="run-logs--list-message">{log.message}</span>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private dateTimeString(dt: Date): string {
    if (!dt) {
      return ''
    }

    const newdate = new Date(dt)
    const formatted = moment(newdate).format(DEFAULT_TIME_FORMAT)

    return formatted
  }
}

export default RunLogRow
