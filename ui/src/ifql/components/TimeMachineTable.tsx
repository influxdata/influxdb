import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'
import {Grid, GridCellProps, AutoSizer, ColumnSizer} from 'react-virtualized'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {FluxTable} from 'src/types'
import {vis} from 'src/ifql/constants'

const NUM_FIXED_ROWS = 1

interface State {
  scrollLeft: number
}

@ErrorHandling
export default class TimeMachineTable extends PureComponent<FluxTable, State> {
  constructor(props) {
    super(props)
    this.state = {
      scrollLeft: 0,
    }
  }

  public render() {
    const {data} = this.props
    const {scrollLeft} = this.state

    return (
      <div style={{flex: '1 1 auto'}}>
        <AutoSizer>
          {({width}) => (
            <ColumnSizer
              width={width}
              columnCount={this.columnCount}
              columnMinWidth={vis.TIME_COLUMN_WIDTH}
            >
              {({adjustedWidth, getColumnWidth}) => (
                <Grid
                  className="table-graph--scroll-window"
                  rowCount={1}
                  fixedRowCount={1}
                  width={adjustedWidth}
                  scrollLeft={scrollLeft}
                  style={this.headerStyle}
                  columnWidth={getColumnWidth}
                  height={vis.TABLE_ROW_HEIGHT}
                  columnCount={this.columnCount}
                  rowHeight={vis.TABLE_ROW_HEIGHT}
                  cellRenderer={this.headerCellRenderer}
                />
              )}
            </ColumnSizer>
          )}
        </AutoSizer>
        <AutoSizer>
          {({height, width}) => (
            <ColumnSizer
              width={width}
              columnMinWidth={vis.TIME_COLUMN_WIDTH}
              columnCount={this.columnCount}
            >
              {({adjustedWidth, getColumnWidth}) => (
                <Grid
                  className="table-graph--scroll-window"
                  width={adjustedWidth}
                  style={this.tableStyle}
                  onScroll={this.handleScroll}
                  columnWidth={getColumnWidth}
                  columnCount={this.columnCount}
                  cellRenderer={this.cellRenderer}
                  rowHeight={vis.TABLE_ROW_HEIGHT}
                  height={height - this.headerOffset}
                  rowCount={data.length - NUM_FIXED_ROWS}
                />
              )}
            </ColumnSizer>
          )}
        </AutoSizer>
      </div>
    )
  }

  private get headerStyle(): CSSProperties {
    // cannot use overflow: hidden overflow-x / overflow-y gets overridden by react-virtualized
    return {overflowX: 'hidden', overflowY: 'hidden'}
  }

  private get tableStyle(): CSSProperties {
    return {marginTop: `${this.headerOffset}px`}
  }

  private get columnCount(): number {
    return _.get(this.props.data, '0', []).length
  }

  private get headerOffset(): number {
    return NUM_FIXED_ROWS * vis.TABLE_ROW_HEIGHT
  }

  private handleScroll = ({scrollLeft}): void => {
    this.setState({scrollLeft})
  }

  private headerCellRenderer = ({
    columnIndex,
    key,
    style,
  }: GridCellProps): React.ReactNode => {
    const {data} = this.props

    return (
      <div
        key={key}
        style={style}
        className="table-graph-cell table-graph-cell__fixed-row"
      >
        {data[0][columnIndex]}
      </div>
    )
  }

  private cellRenderer = ({
    columnIndex,
    key,
    rowIndex,
    style,
  }: GridCellProps): React.ReactNode => {
    const {data} = this.props

    return (
      <div key={key} style={style} className="table-graph-cell">
        {data[rowIndex + NUM_FIXED_ROWS][columnIndex]}
      </div>
    )
  }
}
