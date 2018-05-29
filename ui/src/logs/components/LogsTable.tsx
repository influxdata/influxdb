import moment from 'moment'
import React, {PureComponent} from 'react'
import {Grid, AutoSizer} from 'react-virtualized'
import {getDeep} from 'src/utils/wrappers'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

interface Props {
  data: {
    columns: string[]
    values: string[]
  }
}

const FACILITY_CODES = [
  'kern',
  'user',
  'mail',
  'daemon',
  'auth',
  'syslog',
  'lpr',
  'news',
  'uucp',
  'clock',
  'authpriv',
  'ftp',
  'NTP',
  'log audit',
  'log alert',
  'cron',
  'local0',
  'local1',
  'local2',
  'local3',
  'local4',
  'local5',
  'local6',
  'local7',
]

class LogsTable extends PureComponent<Props> {
  public render() {
    const rowCount = getDeep(this.props, 'data.values.length', 0)
    const columnCount = getDeep(this.props, 'data.columns.length', 1) - 1

    return (
      <div className="logs-viewer--table-container">
        <AutoSizer>
          {({width}) => (
            <Grid
              height={40}
              rowHeight={40}
              rowCount={1}
              width={width}
              cellRenderer={this.headerRenderer}
              columnCount={columnCount}
              columnWidth={this.getColumnWidth}
            />
          )}
        </AutoSizer>
        <AutoSizer>
          {({width, height}) => (
            <FancyScrollbar
              style={{width, height, marginTop: '40px'}}
              autoHide={false}
            >
              <Grid
                height={height}
                rowHeight={40}
                rowCount={rowCount}
                width={width}
                cellRenderer={this.cellRenderer}
                columnCount={columnCount}
                columnWidth={this.getColumnWidth}
              />
            </FancyScrollbar>
          )}
        </AutoSizer>
      </div>
    )
  }

  private severityLevel(value: number): string {
    switch (value) {
      case 0:
        return 'Emergency'
      case 1:
        return 'Alert'
      case 2:
        return 'Critical'
      case 3:
        return 'Error'
      case 4:
        return 'Warning'
      case 5:
        return 'Notice'
      case 6:
        return 'Informational'
      default:
        return 'Debug'
    }
  }

  private getColumnWidth = ({index}: {index: number}) => {
    const column = getDeep<string>(this.props, `data.columns.${index + 1}`, '')

    switch (column) {
      case 'message':
        return 700
      case 'timestamp':
        return 400
      default:
        return 200
    }
  }

  private header(key: string): string {
    return getDeep<string>(
      {
        timestamp: 'Timestamp',
        facility_code: 'Facility',
        procid: 'Proc ID',
        severity_code: 'Severity',
        message: 'Message',
      },
      key,
      ''
    )
  }

  private facility(key: number): string {
    return getDeep<string>(FACILITY_CODES, key, '')
  }

  private headerRenderer = ({key, style, columnIndex}) => {
    const value = getDeep<string>(
      this.props,
      `data.columns.${columnIndex + 1}`,
      ''
    )

    return (
      <div style={style} key={key}>
        {this.header(value)}
      </div>
    )
  }

  private cellRenderer = ({key, style, rowIndex, columnIndex}) => {
    const column = getDeep<string>(
      this.props,
      `data.columns.${columnIndex + 1}`,
      ''
    )

    let value = this.props.data.values[rowIndex][columnIndex + 1]

    switch (column) {
      case 'timestamp':
        value = moment(+value / 1000000).format('YYYY/MM/DD HH:mm:ss')
        break
      case 'severity_code':
        value = this.severityLevel(+value)
        break
      case 'facility_code':
        value = this.facility(+value)
        break
    }

    return (
      <div style={style} key={key}>
        {value}
      </div>
    )
  }
}

export default LogsTable
