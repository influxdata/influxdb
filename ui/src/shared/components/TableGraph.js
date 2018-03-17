import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'

import {MultiGrid} from 'react-virtualized'
import moment from 'moment'

import {timeSeriesToTableGraph} from 'src/utils/timeSeriesToDygraph'
import {
  NULL_COLUMN_INDEX,
  NULL_ROW_INDEX,
  NULL_HOVER_TIME,
  TIME_FORMAT_DEFAULT,
  TIME_COLUMN_DEFAULT,
} from 'src/shared/constants/tableGraph'
import {generateThresholdsListHexs} from 'shared/constants/colorOperations'

const isEmpty = data => data.length <= 1

class TableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      hoveredColumnIndex: NULL_COLUMN_INDEX,
      hoveredRowIndex: NULL_ROW_INDEX,
    }
  }

  componentWillMount() {
    this._data = [[]]
  }

  componentWillUpdate(nextProps) {
    const {data} = timeSeriesToTableGraph(nextProps.data)
    if (_.get(this, ['props', 'tableOptions', 'verticalTimeAxis'], true)) {
      this._data = data
      return
    }
    this._data = _.unzip(data)
  }

  calcHoverTimeRow = (data, hoverTime) =>
    !isEmpty(data) && hoverTime !== NULL_HOVER_TIME
      ? data.findIndex(
          row => row[0] && _.isNumber(row[0]) && row[0] >= hoverTime
        )
      : undefined

  handleHover = (columnIndex, rowIndex) => () => {
    if (this.props.onSetHoverTime) {
      this.props.onSetHoverTime(this._data[rowIndex][0].toString())
      this.setState({
        hoveredColumnIndex: columnIndex,
        hoveredRowIndex: rowIndex,
      })
    }
  }

  handleMouseOut = () => {
    if (this.props.onSetHoverTime) {
      this.props.onSetHoverTime(NULL_HOVER_TIME)
      this.setState({
        hoveredColumnIndex: NULL_COLUMN_INDEX,
        hoveredRowIndex: NULL_ROW_INDEX,
      })
    }
  }

  cellRenderer = ({columnIndex, rowIndex, key, parent, style}) => {
    const data = this._data
    const {hoveredColumnIndex, hoveredRowIndex} = this.state
    const {colors} = this.props

    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const {tableOptions} = this.props
    const timeFormat = tableOptions
      ? tableOptions.timeFormat
      : TIME_FORMAT_DEFAULT
    const columnNames = tableOptions
      ? tableOptions.columnNames
      : [TIME_COLUMN_DEFAULT]

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = rowIndex > 0 && columnIndex === 0
    const isTimeData = isFixedColumn
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const isLastRow = rowIndex === rowCount - 1
    const isLastColumn = columnIndex === columnCount - 1
    const isHighlighted =
      rowIndex === parent.props.scrollToRow ||
      (rowIndex === hoveredRowIndex && hoveredRowIndex !== 0) ||
      (columnIndex === hoveredColumnIndex && hoveredColumnIndex !== 0)
    const dataIsNumerical = _.isNumber(data[rowIndex][columnIndex])

    let cellStyle = style

    if (!isFixedRow && !isFixedColumn && !isFixedCorner) {
      const {bgColor, textColor} = generateThresholdsListHexs(
        colors,
        data[rowIndex][columnIndex]
      )

      cellStyle = {
        ...style,
        backgroundColor: bgColor,
        color: textColor,
      }
    }

    const cellClass = classnames('table-graph-cell', {
      'table-graph-cell__fixed-row': isFixedRow,
      'table-graph-cell__fixed-column': isFixedColumn,
      'table-graph-cell__fixed-corner': isFixedCorner,
      'table-graph-cell__last-row': isLastRow,
      'table-graph-cell__last-column': isLastColumn,
      'table-graph-cell__highlight': isHighlighted,
      'table-graph-cell__numerical': dataIsNumerical,
    })

    const cellData = data[rowIndex][columnIndex]
    const foundColumn = columnNames.find(
      column => column.internalName === cellData
    )
    const columnName =
      foundColumn && (foundColumn.displayName || foundColumn.internalName)

    return (
      <div
        key={key}
        style={cellStyle}
        className={cellClass}
        onMouseOver={this.handleHover(columnIndex, rowIndex)}
      >
        {isTimeData
          ? `${moment(cellData).format(timeFormat)}`
          : columnName || `${cellData}`}
      </div>
    )
  }

  render() {
    const {hoveredColumnIndex, hoveredRowIndex} = this.state
    const {hoverTime, tableOptions, colors} = this.props
    const data = this._data
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_WIDTH = 300
    const ROW_HEIGHT = 30
    const tableWidth = this.gridContainer ? this.gridContainer.clientWidth : 0
    const tableHeight = this.gridContainer ? this.gridContainer.clientHeight : 0
    const hoverTimeRow = this.calcHoverTimeRow(data, hoverTime)

    return (
      <div
        className="table-graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
        onMouseOut={this.handleMouseOut}
      >
        {!isEmpty(data) &&
          <MultiGrid
            columnCount={columnCount}
            columnWidth={COLUMN_WIDTH}
            rowCount={rowCount}
            rowHeight={ROW_HEIGHT}
            height={tableHeight}
            width={tableWidth}
            fixedColumnCount={1}
            fixedRowCount={1}
            enableFixedColumnScroll={true}
            enableFixedRowScroll={true}
            timeFormat={
              tableOptions ? tableOptions.timeFormat : TIME_FORMAT_DEFAULT
            }
            columnNames={
              tableOptions ? tableOptions.columnNames : [TIME_COLUMN_DEFAULT]
            }
            scrollToRow={hoverTimeRow}
            cellRenderer={this.cellRenderer}
            hoveredColumnIndex={hoveredColumnIndex}
            hoveredRowIndex={hoveredRowIndex}
            hoverTime={hoverTime}
            colors={colors}
          />}
      </div>
    )
  }
}

const {arrayOf, number, shape, string, func} = PropTypes

TableGraph.propTypes = {
  cellHeight: number,
  data: arrayOf(shape()),
  tableOptions: shape({}),
  hoverTime: string,
  onSetHoverTime: func,
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
}

export default TableGraph
