import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'
import isEmpty from 'lodash/isEmpty'

import {MultiGrid, ColumnSizer} from 'react-virtualized'
import moment from 'moment'

import {timeSeriesToTableGraph} from 'src/utils/timeSeriesToDygraph'
import {
  NULL_COLUMN_INDEX,
  NULL_ROW_INDEX,
  NULL_HOVER_TIME,
  TIME_FORMAT_DEFAULT,
  TIME_COLUMN_DEFAULT,
  ASCENDING,
  DESCENDING,
  FIX_FIRST_COLUMN_DEFAULT,
  calculateTimeColumnWidth,
} from 'src/shared/constants/tableGraph'
const DEFAULT_SORT = ASCENDING

import {generateThresholdsListHexs} from 'shared/constants/colorOperations'

class TableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      data: [[]],
      unzippedData: [[]],
      hoveredColumnIndex: NULL_COLUMN_INDEX,
      hoveredRowIndex: NULL_ROW_INDEX,
      sortByColumnIndex: NULL_COLUMN_INDEX,
      clickToSortFieldIndex: NULL_COLUMN_INDEX,
      clicktoSortDirection: DEFAULT_SORT,
      timeColumnWidth: calculateTimeColumnWidth(props.tableOptions.timeFormat),
    }
  }

  componentWillReceiveProps(nextProps) {
    const {data} = timeSeriesToTableGraph(nextProps.data)
    const {
      clickToSortFieldIndex,
      clicktoSortDirection,
      sortByColumnIndex,
    } = this.state
    const {tableOptions: {sortBy: {internalName}, timeFormat}} = nextProps
    if (
      sortByColumnIndex === NULL_COLUMN_INDEX ||
      _.get(this.props, ['tableOptions', 'sortBy', 'internalName'], '') !==
        internalName
    ) {
      const newSortByColumnIndex = _.indexOf(data[0], internalName)
      const sortedData = [
        data[0],
        ..._.orderBy(_.drop(data, 1), newSortByColumnIndex, [DEFAULT_SORT]),
      ]
      this.setState({
        data: sortedData,
        unzippedData: _.unzip(sortedData),
        sortByColumnIndex: newSortByColumnIndex,
        clickToSortFieldIndex: NULL_COLUMN_INDEX,
        clicktoSortDirection: DEFAULT_SORT,
        timeColumnWidth: calculateTimeColumnWidth(timeFormat),
      })
      return
    }

    const clicked = clickToSortFieldIndex !== NULL_COLUMN_INDEX
    const sortIndex = clicked ? clickToSortFieldIndex : sortByColumnIndex
    const direction = clicked ? clicktoSortDirection : DEFAULT_SORT
    const sortedData = [
      data[0],
      ..._.orderBy(_.drop(data, 1), sortIndex, [direction]),
    ]
    this.setState({
      data: sortedData,
      unzippedData: _.unzip(sortedData),
      timeColumnWidth: calculateTimeColumnWidth(timeFormat),
    })
  }

  calcHoverTimeIndex = (data, hoverTime, verticalTimeAxis) => {
    if (isEmpty(data) || hoverTime === NULL_HOVER_TIME) {
      return undefined
    }
    if (verticalTimeAxis) {
      return data.findIndex(
        row => row[0] && _.isNumber(row[0]) && row[0] >= hoverTime
      )
    }
    return data[0].findIndex(d => _.isNumber(d) && d >= hoverTime)
  }

  handleHover = (columnIndex, rowIndex) => () => {
    const {onSetHoverTime, tableOptions: {verticalTimeAxis}} = this.props
    const data = verticalTimeAxis ? this.state.data : this.state.unzippedData
    if (onSetHoverTime) {
      const hoverTime = verticalTimeAxis
        ? data[rowIndex][0]
        : data[0][columnIndex]
      onSetHoverTime(hoverTime.toString())
    }
    this.setState({
      hoveredColumnIndex: columnIndex,
      hoveredRowIndex: rowIndex,
    })
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

  handleClickFieldName = (columnIndex, rowIndex) => () => {
    const {tableOptions} = this.props
    const {clickToSortFieldIndex, clicktoSortDirection, data} = this.state
    const verticalTimeAxis = _.get(tableOptions, 'verticalTimeAxis', true)
    const newIndex = verticalTimeAxis ? columnIndex : rowIndex

    if (clickToSortFieldIndex === newIndex) {
      const direction =
        clicktoSortDirection === ASCENDING ? DESCENDING : ASCENDING
      const sortedData = [
        data[0],
        ..._.orderBy(_.drop(data, 1), clickToSortFieldIndex, [direction]),
      ]
      this.setState({
        data: sortedData,
        unzippedData: _.unzip(sortedData),
        clicktoSortDirection: direction,
      })
      return
    }

    const sortedData = [
      data[0],
      ..._.orderBy(_.drop(data, 1), clickToSortFieldIndex, [DEFAULT_SORT]),
    ]
    this.setState({
      data: sortedData,
      unzippedData: _.unzip(sortedData),
      clickToSortFieldIndex: newIndex,
      clicktoSortDirection: DEFAULT_SORT,
    })
  }

  calculateColumnWidth = columnSizerWidth => column => {
    const {index} = column
    const {tableOptions, tableOptions: {verticalTimeAxis}} = this.props
    const {timeColumnWidth} = this.state
    const columnNames = _.get(tableOptions, 'columnNames', [
      TIME_COLUMN_DEFAULT,
    ])

    return verticalTimeAxis && columnNames[index].internalName === 'time'
      ? timeColumnWidth
      : columnSizerWidth
  }

  cellRenderer = ({columnIndex, rowIndex, key, parent, style}) => {
    const {hoveredColumnIndex, hoveredRowIndex} = this.state
    const {tableOptions, colors} = this.props
    const verticalTimeAxis = _.get(tableOptions, 'verticalTimeAxis', true)
    const data = verticalTimeAxis ? this.state.data : this.state.unzippedData
    const timeFormat = _.get(tableOptions, 'timeFormat', TIME_FORMAT_DEFAULT)
    const columnNames = _.get(tableOptions, 'columnNames', [
      TIME_COLUMN_DEFAULT,
    ])
    const fixFirstColumn = _.get(
      tableOptions,
      'fixFirstColumn',
      FIX_FIRST_COLUMN_DEFAULT
    )

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = fixFirstColumn && rowIndex > 0 && columnIndex === 0
    const isTimeData = verticalTimeAxis
      ? rowIndex > 0 && columnIndex === 0
      : isFixedRow
    const isFieldName = verticalTimeAxis ? rowIndex === 0 : columnIndex === 0
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const dataIsNumerical = _.isNumber(data[rowIndex][columnIndex])
    const isHighlightedRow =
      rowIndex === parent.props.scrollToRow ||
      (rowIndex === hoveredRowIndex && hoveredRowIndex !== 0)
    const isHighlightedColumn =
      columnIndex === parent.props.scrollToColumn ||
      (columnIndex === hoveredColumnIndex && hoveredColumnIndex !== 0)

    let cellStyle = style

    if (!isFixedRow && !isFixedColumn && !isFixedCorner) {
      const {bgColor, textColor} = generateThresholdsListHexs({
        colors,
        lastValue: data[rowIndex][columnIndex],
        cellType: 'table',
      })

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
      'table-graph-cell__highlight-row': isHighlightedRow,
      'table-graph-cell__highlight-column': isHighlightedColumn,
      'table-graph-cell__numerical': dataIsNumerical,
      'table-graph-cell__isFieldName': isFieldName,
    })

    const cellData = data[rowIndex][columnIndex]
    const foundColumn = columnNames.find(
      column => column.internalName === cellData
    )
    const columnName =
      foundColumn && (foundColumn.displayName || foundColumn.internalName)

    const cellContents = isTimeData
      ? `${moment(cellData).format(timeFormat)}`
      : columnName || `${cellData}`

    return (
      <div
        key={key}
        style={cellStyle}
        className={cellClass}
        onClick={
          isFieldName ? this.handleClickFieldName(columnIndex, rowIndex) : null
        }
        onMouseOver={this.handleHover(columnIndex, rowIndex)}
        title={cellContents}
      >
        {cellContents}
      </div>
    )
  }

  render() {
    const {
      sortByColumnIndex,
      clickToSortFieldIndex,
      clicktoSortDirection,
      hoveredColumnIndex,
      hoveredRowIndex,
    } = this.state
    const {hoverTime, tableOptions, colors} = this.props
    const verticalTimeAxis = _.get(tableOptions, 'verticalTimeAxis', true)
    const data = verticalTimeAxis ? this.state.data : this.state.unzippedData

    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_MIN_WIDTH = 98
    const COLUMN_MAX_WIDTH = 1000
    const ROW_HEIGHT = 30
    const tableWidth = _.get(this, ['gridContainer', 'clientWidth'], 0)
    const tableHeight = _.get(this, ['gridContainer', 'clientHeight'], 0)
    const hoverTimeIndex =
      hoveredRowIndex === NULL_ROW_INDEX
        ? this.calcHoverTimeIndex(data, hoverTime, verticalTimeAxis)
        : hoveredRowIndex
    const fixedColumnCount = tableOptions.fixFirstColumn ? 1 : undefined
    const hoveringThisTable = hoveredColumnIndex !== NULL_COLUMN_INDEX
    const scrollToRow =
      !hoveringThisTable && verticalTimeAxis ? hoverTimeIndex : undefined
    const scrollToColumn =
      !hoveringThisTable && !verticalTimeAxis ? hoverTimeIndex : undefined

    return (
      <div
        className="table-graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
        onMouseOut={this.handleMouseOut}
      >
        {!isEmpty(data) &&
          <ColumnSizer
            columnCount={columnCount}
            columnMaxWidth={COLUMN_MAX_WIDTH}
            columnMinWidth={COLUMN_MIN_WIDTH}
            width={tableWidth}
          >
            {({getColumnWidth, registerChild}) =>
              <MultiGrid
                ref={registerChild}
                columnCount={columnCount}
                columnWidth={this.calculateColumnWidth(getColumnWidth())}
                rowCount={rowCount}
                rowHeight={ROW_HEIGHT}
                height={tableHeight}
                width={tableWidth}
                fixedColumnCount={fixedColumnCount}
                fixedRowCount={1}
                enableFixedColumnScroll={true}
                enableFixedRowScroll={true}
                timeFormat={
                  tableOptions ? tableOptions.timeFormat : TIME_FORMAT_DEFAULT
                }
                columnNames={
                  tableOptions
                    ? tableOptions.columnNames
                    : [TIME_COLUMN_DEFAULT]
                }
                scrollToRow={scrollToRow}
                scrollToColumn={scrollToColumn}
                verticalTimeAxis={verticalTimeAxis}
                sortByColumnIndex={sortByColumnIndex}
                clickToSortFieldIndex={clickToSortFieldIndex}
                clicktoSortDirection={clicktoSortDirection}
                cellRenderer={this.cellRenderer}
                hoveredColumnIndex={hoveredColumnIndex}
                hoveredRowIndex={hoveredRowIndex}
                hoverTime={hoverTime}
                colors={colors}
                classNameBottomRightGrid="table-graph--scroll-window"
              />}
          </ColumnSizer>}
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
