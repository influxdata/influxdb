import _ from 'lodash'
import moment from 'moment'
import React, {PureComponent, MouseEvent} from 'react'
import {Grid, AutoSizer} from 'react-virtualized'
import {getDeep} from 'src/utils/wrappers'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const ROW_HEIGHT = 40

interface Props {
  data: {
    columns: string[]
    values: string[]
  }
}

interface State {
  scrollLeft: number
  scrollTop: number
}

class LogsTable extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      scrollLeft: 0,
      scrollTop: 0,
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
              height={ROW_HEIGHT}
              rowHeight={ROW_HEIGHT}
              rowCount={1}
              width={width}
              scrollLeft={this.state.scrollLeft}
              onScroll={this.handleHeaderScroll}
              cellRenderer={this.headerRenderer}
              columnCount={columnCount}
              columnWidth={this.getColumnWidth}
            />
          )}
        </AutoSizer>
        <AutoSizer>
          {({width, height}) => (
            <FancyScrollbar
              style={{
                width,
                height,
                marginTop: `${ROW_HEIGHT}px`,
              }}
              setScrollTop={this.handleScrollbarScroll}
              scrollTop={this.state.scrollTop}
              autoHide={true}
            >
              <Grid
                height={height}
                rowHeight={ROW_HEIGHT}
                rowCount={rowCount}
                width={width}
                scrollLeft={this.state.scrollLeft}
                scrollTop={this.state.scrollTop}
                onScroll={this.handleScroll}
                cellRenderer={this.cellRenderer}
                columnCount={columnCount}
                columnWidth={this.getColumnWidth}
                style={{height: 40 * rowCount}}
              />
            </FancyScrollbar>
          )}
        </AutoSizer>
      </div>
    )
  }

  private handleHeaderScroll = ({scrollLeft}) => this.setState({scrollLeft})

  private handleScrollbarScroll = (e: MouseEvent<JSX.Element>) => {
    const {target} = e
    this.handleScroll(target)
  }

  private handleScroll = scrollInfo => {
    const {scrollLeft, scrollTop} = scrollInfo

    this.setState({scrollLeft, scrollTop})
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
        return 900
      case 'timestamp':
        return 200
      case 'procid':
        return 100
      case 'facility':
        return 150
      case 'severity_1':
        return 150
      case 'severity':
        return 24
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
        severity: '',
        severity_1: 'Severity',
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
      case 'severity_1':
        value = this.severityLevel(value)
        break
      case 'message':
        value = _.replace(value, '\\n', '')
        break
      case 'severity':
        return (
          <div style={style} key={key}>
            <div
              className={`logs-viewer--dot ${value}-severity`}
              title={this.severityLevel(value)}
            />
          </div>
        )
    }

    return (
      <div style={style} key={key}>
        {value}
      </div>
    )
  }
}

export default LogsTable
