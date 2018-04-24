import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'

import {MultiGrid, ColumnSizer} from 'react-virtualized'
import moment from 'moment'
import {reduce} from 'fast.js'

const {arrayOf, bool, shape, string, func} = PropTypes

import {
  timeSeriesToTableGraph,
  processTableData,
} from 'src/utils/timeSeriesTransformers'

import {
  NULL_ARRAY_INDEX,
  NULL_HOVER_TIME,
  TIME_FORMAT_DEFAULT,
  TIME_FIELD_DEFAULT,
  ASCENDING,
  DESCENDING,
  DEFAULT_SORT,
  FIX_FIRST_COLUMN_DEFAULT,
  VERTICAL_TIME_AXIS_DEFAULT,
} from 'src/shared/constants/tableGraph'

import {generateThresholdsListHexs} from 'shared/constants/colorOperations'
import {colorsStringSchema} from 'shared/schemas'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class TableGraph extends Component {
  constructor(props) {
    super(props)

    const sortField = _.get(
      this.props,
      ['tableOptions', 'sortBy', 'internalName'],
      TIME_FIELD_DEFAULT.internalName
    )
    this.state = {
      data: [[]],
      processedData: [[]],
      sortedTimeVals: [],
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      hoveredRowIndex: NULL_ARRAY_INDEX,
      sortField,
      sortDirection: DEFAULT_SORT,
      columnWidths: {},
      totalColumnWidths: 0,
    }
  }

  componentWillReceiveProps(nextProps) {
    const {data} = timeSeriesToTableGraph(nextProps.data)
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
    } = nextProps

    let direction, sortFieldName
    if (
      _.get(this.props, ['tableOptions', 'sortBy', 'internalName'], '') ===
      internalName
    ) {
      direction = sortDirection
      sortFieldName = sortField
    } else {
      direction = DEFAULT_SORT
      sortFieldName = internalName
    }

    const {
      processedData,
      sortedTimeVals,
      columnWidths,
      totalWidths,
    } = processTableData(
      data,
      sortFieldName,
      direction,
      verticalTimeAxis,
      fieldNames,
      timeFormat
    )

    this.setState({
      data,
      processedData,
      sortedTimeVals,
      sortField: sortFieldName,
      sortDirection: direction,
      columnWidths,
      totalColumnWidths: totalWidths,
    })
  }

  calcScrollToColRow = () => {
    const {data, sortedTimeVals, hoveredColumnIndex} = this.state
    const {hoverTime, tableOptions} = this.props
    const hoveringThisTable = hoveredColumnIndex !== NULL_ARRAY_INDEX
    const notHovering = hoverTime === NULL_HOVER_TIME
    if (_.isEmpty(data[0]) || notHovering || hoveringThisTable) {
      return {scrollToColumn: undefined, scrollToRow: undefined}
    }

    const firstDiff = Math.abs(hoverTime - sortedTimeVals[1]) // sortedTimeVals[0] is "time"
    const hoverTimeFound = reduce(
      sortedTimeVals,
      (acc, currentTime, index) => {
        const thisDiff = Math.abs(hoverTime - currentTime)
        if (thisDiff < acc.diff) {
          return {index, diff: thisDiff}
        }
        return acc
      },
      {index: 1, diff: firstDiff}
    )

    const {verticalTimeAxis} = tableOptions
    const scrollToColumn = verticalTimeAxis ? undefined : hoverTimeFound.index
    const scrollToRow = verticalTimeAxis ? hoverTimeFound.index : undefined
    return {scrollToRow, scrollToColumn}
  }

  handleHover = (columnIndex, rowIndex) => () => {
    const {
      handleSetHoverTime,
      tableOptions: {verticalTimeAxis},
    } = this.props
    const {sortedTimeVals} = this.state
    if (verticalTimeAxis && rowIndex === 0) {
      return
    }
    if (handleSetHoverTime) {
      const hoverTime = verticalTimeAxis
        ? sortedTimeVals[rowIndex]
        : sortedTimeVals[columnIndex]
      handleSetHoverTime(hoverTime.toString())
    }
    this.setState({
      hoveredColumnIndex: columnIndex,
      hoveredRowIndex: rowIndex,
    })
  }

  handleMouseLeave = () => {
    if (this.props.handleSetHoverTime) {
      this.props.handleSetHoverTime(NULL_HOVER_TIME)
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

    const {processedData, sortedTimeVals} = processTableData(
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
    const {
      tableOptions: {fixFirstColumn},
    } = this.props
    const {processedData, columnWidths, totalColumnWidths} = this.state
    const columnCount = _.get(processedData, ['0', 'length'], 0)
    const columnLabel = processedData[0][index]

    let adjustedColumnSizerWidth = columnWidths[columnLabel]

    const tableWidth = _.get(this, ['gridContainer', 'clientWidth'], 0)
    if (tableWidth > totalColumnWidths) {
      if (columnCount === 1) {
        return columnSizerWidth
      }
      const difference = tableWidth - totalColumnWidths
      const distributeOver =
        fixFirstColumn && columnCount > 1 ? columnCount - 1 : columnCount
      const increment = difference / distributeOver
      adjustedColumnSizerWidth =
        fixFirstColumn && index === 0
          ? columnWidths[columnLabel]
          : columnWidths[columnLabel] + increment
    }

    return adjustedColumnSizerWidth
  }

  cellRenderer = ({columnIndex, rowIndex, key, parent, style}) => {
    const {
      hoveredColumnIndex,
      hoveredRowIndex,
      processedData,
      sortField,
      sortDirection,
    } = this.state
    const {tableOptions, colors} = parent.props

    const {
      timeFormat = TIME_FORMAT_DEFAULT,
      verticalTimeAxis = VERTICAL_TIME_AXIS_DEFAULT,
      fixFirstColumn = FIX_FIRST_COLUMN_DEFAULT,
      fieldNames = [TIME_FIELD_DEFAULT],
    } = tableOptions

    const cellData = processedData[rowIndex][columnIndex]

    const timeFieldIndex = fieldNames.findIndex(
      field => field.internalName === TIME_FIELD_DEFAULT.internalName
    )

    const visibleTime = _.get(fieldNames, [timeFieldIndex, 'visible'], true)

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = fixFirstColumn && rowIndex > 0 && columnIndex === 0
    const isTimeData =
      visibleTime &&
      (verticalTimeAxis
        ? rowIndex !== 0 && columnIndex === timeFieldIndex
        : rowIndex === timeFieldIndex && columnIndex !== 0)
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

    if (!isFixedRow && !isFixedColumn && !isFixedCorner && !isTimeData) {
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
      ? `${moment(cellData).format(
          timeFormat === '' ? TIME_FORMAT_DEFAULT : timeFormat
        )}`
      : fieldName || `${cellData}`

    return (
      <div
        key={key}
        style={cellStyle}
        className={cellClass}
        onClick={isFieldName ? this.handleClickFieldName(cellData) : null}
        onMouseOver={_.throttle(this.handleHover(columnIndex, rowIndex), 100)}
        title={cellContents}
      >
        {cellContents}
      </div>
    )
  }

  getMultiGridRef = (r, registerChild) => {
    this.multiGridRef = r
    return registerChild(r)
  }

  render() {
    const {
      hoveredColumnIndex,
      hoveredRowIndex,
      timeColumnWidth,
      sortField,
      sortDirection,
      processedData,
    } = this.state
    const {hoverTime, tableOptions, colors} = this.props
    const {fixFirstColumn = FIX_FIRST_COLUMN_DEFAULT} = tableOptions
    const columnCount = _.get(processedData, ['0', 'length'], 0)
    const rowCount = columnCount === 0 ? 0 : processedData.length

    const COLUMN_MIN_WIDTH = 100
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
            {({columnWidth, registerChild}) => (
              <MultiGrid
                ref={r => this.getMultiGridRef(r, registerChild)}
                columnCount={columnCount}
                columnWidth={this.calculateColumnWidth(columnWidth)}
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
                timeColumnWidth={timeColumnWidth}
                classNameBottomRightGrid="table-graph--scroll-window"
              />
            )}
          </ColumnSizer>
        )}
      </div>
    )
  }
}

TableGraph.propTypes = {
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
  handleSetHoverTime: func,
  colors: colorsStringSchema,
}

export default TableGraph
