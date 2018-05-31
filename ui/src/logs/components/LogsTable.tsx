import _ from 'lodash'
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

interface State {
  scrollLeft: number
}

class LogsTable extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      scrollLeft: 0,
    }
  }
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
              scrollLeft={this.state.scrollLeft}
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
                onScroll={this.handleScroll}
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

  private handleScroll = scrollInfo => {
    const {scrollLeft} = scrollInfo

    this.setState({scrollLeft})
  }

  private severityLevel(value: string): string {
    switch (value) {
      case 'emerg':
        return 'Emergency'
      case 'alert':
        return 'Alert'
      case 'crit':
        return 'Critical'
      case 'err':
        return 'Error'
      case 'info':
        return 'Informational'
      default:
        return _.capitalize(value)
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
        procid: 'Proc ID',
        message: 'Message',
        appname: 'Application',
      },
      key,
      _.capitalize(key)
    )
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
      case 'severity':
        value = this.severityLevel(value)
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
