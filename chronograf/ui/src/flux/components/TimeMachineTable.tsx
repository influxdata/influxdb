import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'
import {Grid, GridCellProps, AutoSizer, ColumnSizer} from 'react-virtualized'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {FluxTable} from 'src/types'
import {vis} from 'src/flux/constants'

const NUM_FIXED_ROWS = 1

const filterTable = (table: FluxTable): FluxTable => {
  const IGNORED_COLUMNS = ['', 'result', 'table', '_start', '_stop']
  const header = table.data[0]
  const indices = IGNORED_COLUMNS.map(name => header.indexOf(name))
  const data = table.data.map(row =>
    row.filter((__, i) => !indices.includes(i))
  )

  return {
    ...table,
    data,
  }
}

interface Props {
  table: FluxTable
}

interface State {
  scrollLeft: number
  filteredTable: FluxTable
}

@ErrorHandling
export default class TimeMachineTable extends PureComponent<Props, State> {
  public static getDerivedStateFromProps({table}: Props) {
    return {filteredTable: filterTable(table)}
  }

  constructor(props) {
    super(props)

    this.state = {
      scrollLeft: 0,
      filteredTable: filterTable(props.table),
    }
  }

  public render() {
    const {scrollLeft, filteredTable} = this.state

    return (
      <>
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
                  height={vis.TABLE_ROW_HEADER_HEIGHT}
                  columnCount={this.columnCount}
                  rowHeight={vis.TABLE_ROW_HEADER_HEIGHT}
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
                  rowCount={filteredTable.data.length - NUM_FIXED_ROWS}
                />
              )}
            </ColumnSizer>
          )}
        </AutoSizer>
      </>
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
    const {filteredTable} = this.state

    return _.get(filteredTable, 'data.0', []).length
  }

  private get headerOffset(): number {
    return NUM_FIXED_ROWS * vis.TABLE_ROW_HEADER_HEIGHT
  }

  private handleScroll = ({scrollLeft}): void => {
    this.setState({scrollLeft})
  }

  private headerCellRenderer = ({
    columnIndex,
    key,
    style,
  }: GridCellProps): React.ReactNode => {
    const {filteredTable} = this.state

    return (
      <div
        key={key}
        style={{...style, display: 'flex', alignItems: 'center'}}
        className="table-graph-cell table-graph-cell__fixed-row"
      >
        {filteredTable.data[0][columnIndex]}
      </div>
    )
  }

  private cellRenderer = ({
    columnIndex,
    key,
    rowIndex,
    style,
  }: GridCellProps): React.ReactNode => {
    const {filteredTable} = this.state

    return (
      <div key={key} style={style} className="table-graph-cell">
        {filteredTable.data[rowIndex + NUM_FIXED_ROWS][columnIndex]}
      </div>
    )
  }
}
