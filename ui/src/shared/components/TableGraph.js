import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'

import {MultiGrid, ColumnSizer} from 'react-virtualized'
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
  VERTICAL_TIME_AXIS_DEFAULT,
  calculateTimeColumnWidth,
} from 'src/shared/constants/tableGraph'
const DEFAULT_SORT = ASCENDING

import {generateThresholdsListHexs} from 'shared/constants/colorOperations'

const filterInvisibleColumns = (data, fieldNames) => {
  const visibility = {}
  const filteredData = data.map((row, i) => {
    return row.filter((col, j) => {
      if (i === 0) {
        const foundField = fieldNames.find(field => field.internalName === col)
        visibility[j] = foundField ? foundField.visible : true
      }
      return visibility[j]
    })
  })
  return filteredData[0].length ? filteredData : [[]]
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
  const sortedTimeVals = sortedData.map(r => r[0])
  const filteredData = filterInvisibleColumns(sortedData, fieldNames)
  const processedData = verticalTimeAxis ? filteredData : _.unzip(filteredData)

  return {processedData, sortedTimeVals}
}

class TableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      data: [[]],
      processedData: [[]],
      sortedTimeVals: [],
      labels: [],
      timeColumnWidth: calculateTimeColumnWidth(props.tableOptions.timeFormat),
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      hoveredRowIndex: NULL_ARRAY_INDEX,
      sortField: 'time',
      sortDirection: DEFAULT_SORT,
    }
  }

  componentWillReceiveProps(nextProps) {
    const {labels, data} = timeSeriesToTableGraph(nextProps.data)
    if (_.isEmpty(data[0])) {
      return
    }

    const {sortField, sortDirection} = this.state
    const {
      tableOptions: {
        sortBy: {internalName},
        fieldNames,
        verticalTimeAxis,
        timeFormat,
      },
      setDataLabels,
    } = nextProps

    if (timeFormat !== this.props.tableOptions.timeFormat) {
      this.setState({
        timeColumnWidth: calculateTimeColumnWidth(timeFormat),
      })
    }

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

    const {processedData, sortedTimeVals} = processData(
      data,
      sortFieldName,
      direction,
      verticalTimeAxis,
      fieldNames
    )

    this.setState({
      data,
      labels,
      processedData,
      sortedTimeVals,
      sortField: sortFieldName,
      sortDirection: direction,
    })
  }

  calcScrollToColRow = () => {
    const {
      data,
      sortedTimeVals,
      hoveredColumnIndex,
      hoveredRowIndex,
    } = this.state
    const {hoverTime, tableOptions} = this.props
    if (_.isEmpty(data[0]) || hoverTime === NULL_HOVER_TIME) {
      return {scrollToColumn: undefined, scrollToRow: undefined}
    }
    const hoveringThisTable = hoveredColumnIndex !== NULL_ARRAY_INDEX
    if (hoveringThisTable) {
      return {scrollToColumn: hoveredColumnIndex, scrollToRow: hoveredRowIndex}
    }

    const {verticalTimeAxis} = tableOptions

    const hoverTimeIndex = sortedTimeVals.reduce(
      (acc, currentTime, index, array) => {
        const diff = Math.abs(hoverTime - currentTime)
        if (diff === 0) {
          return index
        }
        if (Math.abs(hoverTime - array[acc]) > diff) {
          return index
        }
        return acc
      },
      1
    )
    const scrollToColumn =
      !hoveringThisTable && !verticalTimeAxis ? hoverTimeIndex : undefined
    const scrollToRow =
      !hoveringThisTable && verticalTimeAxis ? hoverTimeIndex : undefined
    return {scrollToRow, scrollToColumn}
  }

  handleHover = (columnIndex, rowIndex) => () => {
    const {onSetHoverTime, tableOptions: {verticalTimeAxis}} = this.props
    const {sortedTimeVals} = this.state
    if (verticalTimeAxis && rowIndex === 0) {
      return
    }
    if (onSetHoverTime) {
      const hoverTime = verticalTimeAxis
        ? sortedTimeVals[rowIndex]
        : sortedTimeVals[columnIndex]
      onSetHoverTime(hoverTime.toString())
    }
    this.setState({
      hoveredColumnIndex: columnIndex,
      hoveredRowIndex: rowIndex,
    })
  }

  handleMouseLeave = () => {
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

    const {processedData, sortedTimeVals} = processData(
      data,
      fieldName,
      direction,
      verticalTimeAxis,
      fieldNames
    )

    this.setState({
      processedData,
      sortedTimeVals,
      sortField: fieldName,
      sortDirection: direction,
    })
  }

  calculateColumnWidth = columnSizerWidth => column => {
    const {index} = column
    const {tableOptions: {verticalTimeAxis}} = this.props
    const {timeColumnWidth, processedData} = this.state

    const labels = verticalTimeAxis
      ? _.unzip(processedData)[0]
      : processedData[0]

    if (labels.length > 0) {
      return verticalTimeAxis && labels[index] === 'time'
        ? timeColumnWidth
        : columnSizerWidth
    }

    return columnSizerWidth
  }

  cellRenderer = ({columnIndex, rowIndex, key, parent, style}) => {
    const {
      hoveredColumnIndex,
      hoveredRowIndex,
      processedData,
      sortField,
      sortDirection,
    } = this.state
    const {tableOptions, colors} = this.props

    const {
      timeFormat = TIME_FORMAT_DEFAULT,
      verticalTimeAxis = VERTICAL_TIME_AXIS_DEFAULT,
      fixFirstColumn = FIX_FIRST_COLUMN_DEFAULT,
      fieldNames = [TIME_FIELD_DEFAULT],
    } = tableOptions

    const cellData = processedData[rowIndex][columnIndex]

    const timeField = fieldNames.find(
      field => field.internalName === TIME_FIELD_DEFAULT.internalName
    )
    const visibleTime = _.get(timeField, 'visible', true)

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = fixFirstColumn && rowIndex > 0 && columnIndex === 0
    const isTimeData =
      visibleTime &&
      (verticalTimeAxis ? rowIndex > 0 && columnIndex === 0 : isFixedRow)
    const isFieldName = verticalTimeAxis ? rowIndex === 0 : columnIndex === 0
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const dataIsNumerical = _.isNumber(cellData)
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
        lastValue: cellData,
        cellType: 'table',
      })

      cellStyle = {
        ...style,
        backgroundColor: bgColor,
        color: textColor,
      }
    }

    const foundField =
      isFieldName && fieldNames.find(field => field.internalName === cellData)
    const fieldName =
      foundField && (foundField.displayName || foundField.internalName)

    const cellClass = classnames('table-graph-cell', {
      'table-graph-cell__fixed-row': isFixedRow,
      'table-graph-cell__fixed-column': isFixedColumn,
      'table-graph-cell__fixed-corner': isFixedCorner,
      'table-graph-cell__highlight-row': isHighlightedRow,
      'table-graph-cell__highlight-column': isHighlightedColumn,
      'table-graph-cell__numerical': dataIsNumerical,
      'table-graph-cell__field-name': isFieldName,
      'table-graph-cell__sort-asc':
        isFieldName && sortField === cellData && sortDirection === ASCENDING,
      'table-graph-cell__sort-desc':
        isFieldName && sortField === cellData && sortDirection === DESCENDING,
    })

    const cellContents = isTimeData
      ? `${moment(cellData).format(timeFormat)}`
      : fieldName || `${cellData}`

    return (
      <div
        key={key}
        style={cellStyle}
        className={cellClass}
        onClick={isFieldName ? this.handleClickFieldName(cellData) : null}
        onMouseOver={this.handleHover(columnIndex, rowIndex)}
        title={cellContents}
      >
        {cellContents}
      </div>
    )
  }

  render() {
    const {
      hoveredColumnIndex,
      hoveredRowIndex,
      sortField,
      sortDirection,
      processedData,
    } = this.state
    const {hoverTime, tableOptions, colors} = this.props
    const {fixFirstColumn = FIX_FIRST_COLUMN_DEFAULT} = tableOptions
    const columnCount = _.get(processedData, ['0', 'length'], 0)
    const rowCount = columnCount === 0 ? 0 : processedData.length

    const COLUMN_MIN_WIDTH = 98
    const COLUMN_MAX_WIDTH = 1000
    const ROW_HEIGHT = 30

    const fixedColumnCount = fixFirstColumn && columnCount > 1 ? 1 : undefined

    const tableWidth = _.get(this, ['gridContainer', 'clientWidth'], 0)
    const tableHeight = _.get(this, ['gridContainer', 'clientHeight'], 0)
    const {scrollToColumn, scrollToRow} = this.calcScrollToColRow()
    return (
      <div
        className="table-graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
        onMouseLeave={this.handleMouseLeave}
      >
        {rowCount > 0 && (
          <ColumnSizer
            columnCount={columnCount}
            columnMaxWidth={COLUMN_MAX_WIDTH}
            columnMinWidth={COLUMN_MIN_WIDTH}
            width={tableWidth}
          >
            {({getColumnWidth, registerChild}) => (
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
                scrollToRow={scrollToRow}
                scrollToColumn={scrollToColumn}
                sortField={sortField}
                sortDirection={sortDirection}
                cellRenderer={this.cellRenderer}
                hoveredColumnIndex={hoveredColumnIndex}
                hoveredRowIndex={hoveredRowIndex}
                hoverTime={hoverTime}
                colors={colors}
                tableOptions={tableOptions}
                classNameBottomRightGrid="table-graph--scroll-window"
              />
            )}
          </ColumnSizer>
        )}
      </div>
    )
  }
}

const {arrayOf, bool, number, shape, string, func} = PropTypes

TableGraph.propTypes = {
  cellHeight: number,
  data: arrayOf(shape()),
  tableOptions: shape({
    timeFormat: string.isRequired,
    verticalTimeAxis: bool.isRequired,
    sortBy: shape({
      internalName: string.isRequired,
      displayName: string.isRequired,
      visible: bool.isRequired,
    }).isRequired,
    wrapping: string.isRequired,
    fieldNames: arrayOf(
      shape({
        internalName: string.isRequired,
        displayName: string.isRequired,
        visible: bool.isRequired,
      })
    ).isRequired,
    fixFirstColumn: bool,
  }),
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
