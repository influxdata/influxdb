import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'
import {timeSeriesToTable} from 'src/utils/timeSeriesToDygraph'
import {MultiGrid} from 'react-virtualized'
import moment from 'moment'

class TableGraph extends Component {
  state = {timeFormat: 'MM/DD/YYYY HH:mm:ss.ss'}

  componentWillMount() {
    this._labels = []
    this._data = [[]]
  }

  componentWillUpdate(nextProps) {
    // TODO: determine if in dataExplorer
    const {labels, data} = timeSeriesToTable(nextProps.data)
    this._labels = labels
    this._data = data
  }

  cellRenderer = ({columnIndex, key, rowIndex, style}) => {
    const data = this._data
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const {timeFormat} = this.state
    const isTimeCell = columnIndex === 0 && rowIndex > 0

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = rowIndex > 0 && columnIndex === 0
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const isLastRow = rowIndex === rowCount - 1
    const isLastColumn = columnIndex === columnCount - 1

    const cellClass = classnames('table-graph-cell', {
      'table-graph-cell__fixed-row': isFixedRow,
      'table-graph-cell__fixed-column': isFixedColumn,
      'table-graph-cell__fixed-corner': isFixedCorner,
      'table-graph-cell__last-row': isLastRow,
      'table-graph-cell__last-column': isLastColumn,
    })

    return (
      <div key={key} className={cellClass} style={style}>
        {isTimeCell
          ? moment(data[rowIndex][columnIndex]).format(timeFormat)
          : data[rowIndex][columnIndex]}
      </div>
    )
  }

  render() {
    const data = this._data
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_WIDTH = 300
    const ROW_HEIGHT = 30
    const tableWidth = this.gridContainer ? this.gridContainer.clientWidth : 0
    const tableHeight = this.gridContainer ? this.gridContainer.clientHeight : 0

    return (
      <div
        className="table-graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
      >
        {data.length > 1 &&
          <MultiGrid
            fixedColumnCount={1}
            fixedRowCount={1}
            cellRenderer={this.cellRenderer}
            columnCount={columnCount}
            columnWidth={COLUMN_WIDTH}
            height={tableHeight}
            rowCount={rowCount}
            rowHeight={ROW_HEIGHT}
            width={tableWidth}
            enableFixedColumnScroll={true}
            enableFixedRowScroll={true}
          />}
      </div>
    )
  }
}

const {arrayOf, number, shape} = PropTypes

TableGraph.propTypes = {
  cellHeight: number,
  data: arrayOf(shape()),
}

export default TableGraph
