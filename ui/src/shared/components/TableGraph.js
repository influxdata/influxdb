import React, {Component} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'
import classnames from 'classnames'
import {connect} from 'react-redux'

import {MultiGrid, ColumnSizer} from 'react-virtualized'
import {bindActionCreators} from 'redux'
import moment from 'moment'
import {reduce} from 'fast.js'

const {arrayOf, bool, shape, string, func} = PropTypes

import {timeSeriesToTableGraph} from 'src/utils/timeSeriesTransformers'
import {
  computeFieldOptions,
  transformTableData,
} from 'src/dashboards/utils/tableGraph'
import {updateFieldOptions} from 'src/dashboards/actions/cellEditorOverlay'

import {
  ASCENDING,
  DESCENDING,
  NULL_HOVER_TIME,
  NULL_ARRAY_INDEX,
  DEFAULT_TIME_FIELD,
  DEFAULT_TIME_FORMAT,
  DEFAULT_SORT_DIRECTION,
  DEFAULT_FIX_FIRST_COLUMN,
  DEFAULT_VERTICAL_TIME_AXIS,
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
      DEFAULT_TIME_FIELD.internalName
    )

    this.state = {
      data: [[]],
      transformedData: [[]],
      sortedTimeVals: [],
      sortedLabels: [],
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      hoveredRowIndex: NULL_ARRAY_INDEX,
      sort: {field: sortField, direction: DEFAULT_SORT_DIRECTION},
      columnWidths: {},
      totalColumnWidths: 0,
    }
  }

  handleUpdateFieldOptions = fieldOptions => {
    const {isInCEO} = this.props
    if (!isInCEO) {
      return
    }
    this.props.handleUpdateFieldOptions(fieldOptions)
  }

  componentWillReceiveProps(nextProps) {
    const updatedProps = _.keys(nextProps).filter(
      k => !_.isEqual(this.props[k], nextProps[k])
    )
    const {tableOptions, fieldOptions, timeFormat} = nextProps

    let result = {}

    if (_.includes(updatedProps, 'data')) {
      result = timeSeriesToTableGraph(nextProps.data, nextProps.queryASTs)
    }

    const data = _.get(result, 'data', this.state.data)
    const sortedLabels = _.get(result, 'sortedLabels', this.state.sortedLabels)

    const computedFieldOptions = computeFieldOptions(fieldOptions, sortedLabels)

    if (_.includes(updatedProps, 'data')) {
      this.handleUpdateFieldOptions(computedFieldOptions)
    }

    if (_.isEmpty(data[0])) {
      return
    }

    const {sort} = this.state
    const internalName = _.get(
      nextProps,
      ['tableOptions', 'sortBy', 'internalName'],
      ''
    )
    if (
      !_.get(this.props, ['tableOptions', 'sortBy', 'internalName'], '') ===
      internalName
    ) {
      sort.direction = DEFAULT_SORT_DIRECTION
      sort.field = internalName
    }

    if (
      _.includes(updatedProps, 'data') ||
      _.includes(updatedProps, 'tableOptions') ||
      _.includes(updatedProps, 'fieldOptions') ||
      _.includes(updatedProps, 'timeFormat')
    ) {
      const {
        transformedData,
        sortedTimeVals,
        columnWidths,
        totalWidths,
      } = transformTableData(
        data,
        sort,
        computedFieldOptions,
        tableOptions,
        timeFormat
      )

      this.setState({
        data,
        sortedLabels,
        transformedData,
        sortedTimeVals,
        sort,
        columnWidths,
        totalColumnWidths: totalWidths,
      })
    }
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
    const {data, sort} = this.state
    const {tableOptions, timeFormat, fieldOptions} = this.props

    if (fieldName === sort.field) {
      sort.direction = sort.direction === ASCENDING ? DESCENDING : ASCENDING
    } else {
      sort.field = fieldName
      sort.direction = DEFAULT_SORT_DIRECTION
    }

    const {transformedData, sortedTimeVals} = transformTableData(
      data,
      sort,
      fieldOptions,
      tableOptions,
      timeFormat
    )

    this.setState({
      transformedData,
      sortedTimeVals,
      sort,
    })
  }

  calculateColumnWidth = columnSizerWidth => column => {
    const {index} = column
    const {
      tableOptions: {fixFirstColumn},
    } = this.props
    const {transformedData, columnWidths, totalColumnWidths} = this.state
    const columnCount = _.get(transformedData, ['0', 'length'], 0)
    const columnLabel = transformedData[0][index]

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
      transformedData,
      sort,
    } = this.state

    const {
      fieldOptions = [DEFAULT_TIME_FIELD],
      timeFormat = DEFAULT_TIME_FORMAT,
      tableOptions,
      colors,
    } = parent.props

    const {
      verticalTimeAxis = DEFAULT_VERTICAL_TIME_AXIS,
      fixFirstColumn = DEFAULT_FIX_FIRST_COLUMN,
    } = tableOptions

    const cellData = transformedData[rowIndex][columnIndex]

    const timeFieldIndex = fieldOptions.findIndex(
      field => field.internalName === DEFAULT_TIME_FIELD.internalName
    )

    const visibleTime = _.get(fieldOptions, [timeFieldIndex, 'visible'], true)

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
      isFieldName && fieldOptions.find(field => field.internalName === cellData)
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
        isFieldName && sort.field === cellData && sort.direction === ASCENDING,
      'table-graph-cell__sort-desc':
        isFieldName && sort.field === cellData && sort.direction === DESCENDING,
    })

    const cellContents = isTimeData
      ? `${moment(cellData).format(
          timeFormat === '' ? DEFAULT_TIME_FORMAT : timeFormat
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
      sort,
      transformedData,
    } = this.state
    const {
      hoverTime,
      tableOptions,
      colors,
      fieldOptions,
      timeFormat,
    } = this.props
    const {fixFirstColumn = DEFAULT_FIX_FIRST_COLUMN} = tableOptions
    const columnCount = _.get(transformedData, ['0', 'length'], 0)
    const rowCount = columnCount === 0 ? 0 : transformedData.length

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
                cellRenderer={this.cellRenderer}
                hoveredColumnIndex={hoveredColumnIndex}
                hoveredRowIndex={hoveredRowIndex}
                hoverTime={hoverTime}
                colors={colors}
                sort={sort}
                fieldOptions={fieldOptions}
                tableOptions={tableOptions}
                timeFormat={timeFormat}
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
    verticalTimeAxis: bool.isRequired,
    sortBy: shape({
      internalName: string.isRequired,
      displayName: string.isRequired,
      visible: bool.isRequired,
    }).isRequired,
    wrapping: string.isRequired,
    fixFirstColumn: bool,
  }),
  timeFormat: string.isRequired,
  fieldOptions: arrayOf(
    shape({
      internalName: string.isRequired,
      displayName: string.isRequired,
      visible: bool.isRequired,
    })
  ).isRequired,
  hoverTime: string,
  handleUpdateFieldOptions: func,
  handleSetHoverTime: func,
  colors: colorsStringSchema,
  queryASTs: arrayOf(shape()),
  isInCEO: bool,
}

const mapDispatchToProps = dispatch => ({
  handleUpdateFieldOptions: bindActionCreators(updateFieldOptions, dispatch),
})

export default connect(null, mapDispatchToProps)(TableGraph)
