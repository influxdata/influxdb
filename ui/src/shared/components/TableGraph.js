import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

import {timeSeriesToTableGraph} from 'src/utils/timeSeriesToDygraph'

import {MultiGrid} from 'react-virtualized'

class TableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      hoveredColumnIndex: -1,
      hoveredRowIndex: -1,
    }
  }

  componentWillMount() {
    this._labels = []
    this._data = [[]]
  }

  componentWillUpdate(nextProps) {
    const {labels, data} = timeSeriesToTableGraph(nextProps.data)
    this._labels = labels
    this._data = data
  }

  handleHover = (columnIndex, rowIndex) => () => {
    this.props.onSetHoverTime(this._data[rowIndex][0].toString())
    this.setState({hoveredColumnIndex: columnIndex, hoveredRowIndex: rowIndex})
  }

  handleMouseOut = () => {
    this.props.onSetHoverTime('0')
    this.setState({hoveredColumnIndex: -1, hoveredRowIndex: -1})
  }

  cellRenderer = ({columnIndex, rowIndex, key, style, parent}) => {
    const data = this._data
    const {hoveredColumnIndex, hoveredRowIndex} = this.state

    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = rowIndex > 0 && columnIndex === 0
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const isLastRow = rowIndex === rowCount - 1
    const isLastColumn = columnIndex === columnCount - 1
    const isHovered =
      rowIndex === parent.props.scrollToRow ||
      rowIndex === hoveredRowIndex ||
      columnIndex === hoveredColumnIndex

    const cellClass = classnames('table-graph-cell', {
      'table-graph-cell__fixed-row': isFixedRow,
      'table-graph-cell__fixed-column': isFixedColumn,
      'table-graph-cell__fixed-corner': isFixedCorner,
      'table-graph-cell__last-row': isLastRow,
      'table-graph-cell__last-column': isLastColumn,
      'table-graph-cell__hovered': isHovered,
    })

    return (
      <div
        key={key}
        style={style}
        className={cellClass}
        onMouseOver={this.handleHover(columnIndex, rowIndex)}
      >
        {`${data[rowIndex][columnIndex]}`}
      </div>
    )
  }

  render() {
    const {hoveredColumnIndex, hoveredRowIndex} = this.state
    const {hoverTime} = this.props
    const data = this._data
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_WIDTH = 300
    const ROW_HEIGHT = 30
    const tableWidth = this.gridContainer ? this.gridContainer.clientWidth : 0
    const tableHeight = this.gridContainer ? this.gridContainer.clientHeight : 0

    const hoverTimeRow =
      data.length > 1 && hoverTime > 0
        ? data.findIndex(
            row => row[0] && _.isNumber(row[0]) && row[0] >= hoverTime
          )
        : undefined

    return (
      <div
        className="table-graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
        onMouseOut={this.handleMouseOut}
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
            enableFixedColumnScroll={true}
            enableFixedRowScroll={true}
            hoveredColumnIndex={hoveredColumnIndex}
            hoveredRowIndex={hoveredRowIndex}
            scrollToRow={hoverTimeRow}
            hoverTime={hoverTime}
            width={tableWidth}
          />}
      </div>
    )
  }
}

const {arrayOf, number, shape, string, func} = PropTypes

TableGraph.propTypes = {
  cellHeight: number,
  data: arrayOf(shape()),
  hoverTime: string,
  onSetHoverTime: func,
}

export default TableGraph
