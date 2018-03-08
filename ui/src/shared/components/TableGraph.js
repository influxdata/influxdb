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
    // TODO: determine if in dataExplorer
    const {labels, data} = timeSeriesToTableGraph(nextProps.data)
    this._labels = labels
    this._data = data
  }

  handleHover = (columnIndex, rowIndex) => () => {
    const {onSetHoverTime} = this.props
    onSetHoverTime(this._data[rowIndex][0].toString())
    this.setState({hoveredColumnIndex: columnIndex, hoveredRowIndex: rowIndex})
  }

  handleMouseOut = () => {
    const {onSetHoverTime} = this.props
    onSetHoverTime('0')
    this.setState({hoveredColumnIndex: -1, hoveredRowIndex: -1})
  }

  cellRenderer = ({columnIndex, key, rowIndex, style}) => {
    const data = this._data
    const {hoverTime} = this.props
    const {hoveredColumnIndex, hoveredRowIndex} = this.state

    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = rowIndex > 0 && columnIndex === 0
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const isLastRow = rowIndex === rowCount - 1
    const isLastColumn = columnIndex === columnCount - 1
    const isHovered =
      data[rowIndex][0] === hoverTime ||
      columnIndex === hoveredColumnIndex ||
      rowIndex === hoveredRowIndex

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
        {data[rowIndex][columnIndex]
          ? data[rowIndex][columnIndex].toString()
          : 'null'}
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
            hoveredColumnIndex={this.state.hoveredColumnIndex}
            hoveredRowIndex={this.state.hoveredRowIndex}
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
