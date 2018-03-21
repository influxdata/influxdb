import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'
import isEmpty from 'lodash/isEmpty'

import {MultiGrid} from 'react-virtualized'
import moment from 'moment'

import {timeSeriesToTableGraph} from 'src/utils/timeSeriesToDygraph'
import {
  NULL_ARRAY_INDEX,
  NULL_HOVER_TIME,
  TIME_FORMAT_DEFAULT,
  TIME_FIELD_DEFAULT,
  ASCENDING,
  DESCENDING,
  FIX_FIRST_COLUMN_DEFAULT,
} from 'src/shared/constants/tableGraph'
const DEFAULT_SORT = ASCENDING

import {generateThresholdsListHexs} from 'shared/constants/colorOperations'

const filterInvisibleRows = (data, fieldNames) => {
  const visibleData = data.filter(row => {
    const foundField = fieldNames.find(field => field.internalName === row[0])
    return foundField && foundField.visible
  })

  return visibleData.length ? visibleData : [[]]
}

const filterInvisibleColumns = (data, fieldNames) => {
  const visibleColumns = {}
  const visibleData = data.map((row, i) => {
    return row.filter((col, j) => {
      if (i === 0) {
        const foundField = fieldNames.find(field => field.internalName === col)
        visibleColumns[j] = foundField && foundField.visible
      }
      return visibleColumns[j]
    })
  })
  return visibleData[0].length ? visibleData : [[]]
}

const processData = (
  data,
  sortFieldName,
  direction,
  verticalTimeAxis,
  fieldNames
) => {
  const sortIndex = _.indexOf(data[0], sortFieldName)
  const sortedData = [
    data[0],
    ..._.orderBy(_.drop(data, 1), sortIndex, [direction]),
  ]
  const visibleData = verticalTimeAxis
    ? filterInvisibleColumns(sortedData, fieldNames)
    : filterInvisibleRows(sortedData, fieldNames)
  return {sortedData, unzippedData: _.unzip(sortedData), visibleData}
}

class TableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      data: [[]],
      unzippedData: [[]],
      visibleData: [[]],
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      hoveredRowIndex: NULL_ARRAY_INDEX,
      sortField: '',
      sortDirection: DEFAULT_SORT,
    }
  }

  componentWillReceiveProps(nextProps) {
    const {labels, data} = timeSeriesToTableGraph(nextProps.data)
    if (isEmpty(data[0])) {
      return
    }
    const {sortField, sortDirection} = this.state
    const {
      tableOptions: {sortBy: {internalName}, fieldNames, verticalTimeAxis},
      setDataLabels,
    } = nextProps

    if (setDataLabels) {
      setDataLabels(labels)
    }

    let direction, sortFieldName
    if (
      _.isEmpty(sortField) ||
      _.get(this.props, ['tableOptions', 'sortBy', 'internalName'], '') !==
        _.get(nextProps, ['tableOptions', 'sortBy', 'internalName'], '')
    ) {
      direction = DEFAULT_SORT
      sortFieldName = internalName
    } else {
      direction = sortDirection
      sortFieldName = sortField
    }

    const {sortedData, unzippedData, visibleData} = processData(
      data,
      sortFieldName,
      direction,
      verticalTimeAxis,
      fieldNames
    )

    this.setState({
      data: sortedData,
      visibleData,
      unzippedData,
      sortField: sortFieldName,
      sortDirection: direction,
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
        hoveredColumnIndex: NULL_ARRAY_INDEX,
        hoveredRowIndex: NULL_ARRAY_INDEX,
      })
    }
  }

  handleClickFieldName = fieldName => () => {
    const {tableOptions} = this.props
    const {data, sortField, sortDirection} = this.state
    const verticalTimeAxis = _.get(tableOptions, 'verticalTimeAxis', true)
    const fieldNames = _.get(tableOptions, 'fieldNames', [TIME_FIELD_DEFAULT])

    let direction
    if (fieldName === sortField) {
      direction = sortDirection === ASCENDING ? DESCENDING : ASCENDING
    } else {
      direction = DEFAULT_SORT
    }

    const {sortedData, unzippedData, visibleData} = processData(
      data,
      fieldName,
      direction,
      verticalTimeAxis,
      fieldNames
    )

    this.setState({
      data: sortedData,
      unzippedData,
      visibleData,
      sortField: fieldName,
      sortDirection: direction,
    })
  }

  cellRenderer = ({columnIndex, rowIndex, key, parent, style}) => {
    const {hoveredColumnIndex, hoveredRowIndex} = this.state
    const {tableOptions, colors} = this.props
    const verticalTimeAxis = _.get(tableOptions, 'verticalTimeAxis', true)
    const data = verticalTimeAxis ? this.state.data : this.state.unzippedData
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const timeFormat = _.get(tableOptions, 'timeFormat', TIME_FORMAT_DEFAULT)
    const fieldNames = _.get(tableOptions, 'fieldNames', [TIME_FIELD_DEFAULT])
    const fixFirstColumn = _.get(
      tableOptions,
      'fixFirstColumn',
      FIX_FIRST_COLUMN_DEFAULT
    )

    const timeField = fieldNames.find(
      field => field.internalName === TIME_FIELD_DEFAULT.internalName
    )

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = fixFirstColumn && rowIndex > 0 && columnIndex === 0
    const isTimeData =
      timeField.visible &&
      (verticalTimeAxis ? rowIndex > 0 && columnIndex === 0 : isFixedRow)
    const isFieldName = verticalTimeAxis ? rowIndex === 0 : columnIndex === 0

    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const isLastRow = rowIndex === rowCount - 1
    const isLastColumn = columnIndex === columnCount - 1
    const isHighlighted =
      rowIndex === parent.props.scrollToRow ||
      columnIndex === parent.props.scrollToColumn ||
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
      'table-graph-cell__isFieldName': isFieldName,
    })

    const cellData = data[rowIndex][columnIndex]

    const foundField = fieldNames.find(field => field.internalName === cellData)
    const fieldName =
      foundField && (foundField.displayName || foundField.internalName)

    return (
      <div
        key={key}
        style={cellStyle}
        className={cellClass}
        onClick={isFieldName ? this.handleClickFieldName(cellData) : null}
        onMouseOver={this.handleHover(columnIndex, rowIndex)}
      >
        {isTimeData
          ? `${moment(cellData).format(timeFormat)}`
          : fieldName || `${cellData}`}
      </div>
    )
  }

  render() {
    const {
      hoveredColumnIndex,
      hoveredRowIndex,
      sortField,
      sortDirection,
    } = this.state
    const {hoverTime, tableOptions, colors} = this.props
    const verticalTimeAxis = _.get(tableOptions, 'verticalTimeAxis', true)

    const data = verticalTimeAxis
      ? this.state.visibleData
      : this.state.unzippedData

    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_WIDTH = 300
    const ROW_HEIGHT = 30
    const tableWidth = _.get(this, ['gridContainer', 'clientWidth'], 0)
    const tableHeight = _.get(this, ['gridContainer', 'clientHeight'], 0)
    const hoverTimeIndex =
      hoveredRowIndex === NULL_ARRAY_INDEX
        ? this.calcHoverTimeIndex(data, hoverTime, verticalTimeAxis)
        : hoveredRowIndex
    const fixedColumnCount = tableOptions.fixFirstColumn ? 1 : undefined
    const hoveringThisTable = hoveredColumnIndex !== NULL_ARRAY_INDEX
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
          <MultiGrid
            columnCount={columnCount}
            columnWidth={COLUMN_WIDTH}
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
            fieldNames={
              tableOptions ? tableOptions.fieldNames : [TIME_FIELD_DEFAULT]
            }
            scrollToRow={scrollToRow}
            scrollToColumn={scrollToColumn}
            verticalTimeAxis={verticalTimeAxis}
            sortField={sortField}
            sortDirection={sortDirection}
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
  setDataLabels: func,
}

export default TableGraph
