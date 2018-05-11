import React, {Component} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import {connect} from 'react-redux'

import {ColumnSizer} from 'react-virtualized'
import {MultiGrid} from 'src/shared/components/MultiGrid'
import {bindActionCreators} from 'redux'
import moment from 'moment'
import {reduce} from 'fast.js'

import {timeSeriesToTableGraph} from 'src/utils/timeSeriesTransformers'
import {
  computeFieldOptions,
  transformTableData,
} from 'src/dashboards/utils/tableGraph'
import {updateFieldOptions} from 'src/dashboards/actions/cellEditorOverlay'
import {DEFAULT_TIME_FIELD} from 'src/dashboards/constants'
import {
  ASCENDING,
  DESCENDING,
  NULL_HOVER_TIME,
  NULL_ARRAY_INDEX,
  DEFAULT_FIX_FIRST_COLUMN,
  DEFAULT_VERTICAL_TIME_AXIS,
  DEFAULT_SORT_DIRECTION,
} from 'src/shared/constants/tableGraph'
import {generateThresholdsListHexs} from 'src/shared/constants/colorOperations'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {TimeSeriesServerResponse} from 'src/types/series'
import {ColorString} from 'src/types/colors'
import {
  TableOptions,
  FieldOption,
  DecimalPlaces,
  Sort,
  DbData,
} from 'src/types/dashboard'

interface Label {
  label: string
  seriesIndex: number
  responseIndex: number
}

interface Props {
  data: TimeSeriesServerResponse
  tableOptions: TableOptions
  timeFormat: string
  decimalPlaces: DecimalPlaces
  fieldOptions: FieldOption[]
  hoverTime: string
  handleUpdateFieldOptions: (fieldOptions: FieldOption[]) => void
  handleSetHoverTime: (hovertime: string) => void
  colors: ColorString
  isInCEO: boolean
}

interface State {
  data: DbData[][]
  transformedData: DbData[][]
  sortedTimeVals: DbData[]
  sortedLabels: Label[]
  hoveredColumnIndex: number
  hoveredRowIndex: number
  timeColumnWidth: number
  sort: Sort
  columnWidths: ColumnWidths
  totalColumnWidths: number
  isTimeVisible: boolean
}

interface ColumnWidths {
  totalWidths: number
  widths: {[x: string]: number}
}

@ErrorHandling
class TableGraph extends Component<Props, State> {
  private gridContainer: HTMLDivElement
  constructor(props) {
    super(props)

    const sortField: string = _.get(
      this.props,
      'tableOptions.sortBy.internalName',
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
      columnWidths: {totalWidths: 0, widths: {}},
      totalColumnWidths: 0,
      isTimeVisible: true,
      timeColumnWidth: 0,
    }
  }

  public render() {
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
      decimalPlaces,
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
    const {scrollToColumn, scrollToRow} = this.scrollToColRow
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
                sort={sort}
                cellRenderer={this.cellRenderer}
                hoveredColumnIndex={hoveredColumnIndex}
                hoveredRowIndex={hoveredRowIndex}
                hoverTime={hoverTime}
                colors={colors}
                fieldOptions={fieldOptions}
                tableOptions={tableOptions}
                timeFormat={timeFormat}
                decimalPlaces={decimalPlaces}
                timeColumnWidth={timeColumnWidth}
                classNameBottomRightGrid="table-graph--scroll-window"
              />
            )}
          </ColumnSizer>
        )}
      </div>
    )
  }

  public componentDidMount() {
    const sortField: string = _.get(
      this.props,
      ['tableOptions', 'sortBy', 'internalName'],
      DEFAULT_TIME_FIELD.internalName
    )

    const sort: Sort = {field: sortField, direction: DEFAULT_SORT_DIRECTION}
    const {
      data,
      tableOptions,
      timeFormat,
      fieldOptions,
      decimalPlaces,
    } = this.props
    const result = timeSeriesToTableGraph(data)
    const sortedLabels = result.sortedLabels

    const computedFieldOptions = computeFieldOptions(fieldOptions, sortedLabels)

    if (!_.isEqual(computedFieldOptions, fieldOptions)) {
      this.handleUpdateFieldOptions(computedFieldOptions)
    }
    const {transformedData, sortedTimeVals, columnWidths} = transformTableData(
      result.data,
      sort,
      computedFieldOptions,
      tableOptions,
      timeFormat,
      decimalPlaces
    )

    const timeField = _.find(
      fieldOptions,
      f => f.internalName === DEFAULT_TIME_FIELD.internalName
    )
    const isTimeVisible = _.get(timeField, 'visible', this.state.isTimeVisible)

    this.setState({
      transformedData,
      sortedTimeVals,
      columnWidths: columnWidths.widths,
      data: result.data,
      sortedLabels,
      totalColumnWidths: totalWidths,
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      hoveredRowIndex: NULL_ARRAY_INDEX,
      sort,
      isTimeVisible,
    })
  }

  public componentWillReceiveProps(nextProps) {
    const updatedProps = _.keys(nextProps).filter(
      k => !_.isEqual(this.props[k], nextProps[k])
    )
    const {tableOptions, fieldOptions, timeFormat, decimalPlaces} = nextProps

    let result = {}

    if (_.includes(updatedProps, 'data')) {
      result = timeSeriesToTableGraph(nextProps.data)
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
        timeFormat,
        decimalPlaces
      )

      let isTimeVisible = this.state.isTimeVisible
      if (_.includes(updatedProps, 'fieldOptions')) {
        const timeField = _.find(
          nextProps.fieldOptions,
          f => f.internalName === DEFAULT_TIME_FIELD.internalName
        )
        isTimeVisible = _.get(timeField, 'visible', this.state.isTimeVisible)
      }

      this.setState({
        data,
        sortedLabels,
        transformedData,
        sortedTimeVals,
        sort,
        columnWidths,
        totalColumnWidths: totalWidths,
        isTimeVisible,
      })
    }
  }

  private handleUpdateFieldOptions = (fieldOptions: FieldOption[]): void => {
    const {isInCEO} = this.props
    if (!isInCEO) {
      return
    }
    this.props.handleUpdateFieldOptions(fieldOptions)
  }

  private get scrollToColRow(): {
    scrollToRow: number | null
    scrollToColumn: number | null
  } {
    const {data, sortedTimeVals, hoveredColumnIndex, isTimeVisible} = this.state
    const {hoverTime, tableOptions} = this.props
    const hoveringThisTable = hoveredColumnIndex !== NULL_ARRAY_INDEX
    const notHovering = hoverTime === NULL_HOVER_TIME
    if (
      _.isEmpty(data[0]) ||
      notHovering ||
      hoveringThisTable ||
      !isTimeVisible
    ) {
      return {scrollToColumn: null, scrollToRow: null}
    }

    const firstDiff = Math.abs(Number(hoverTime) - sortedTimeVals[1]) // sortedTimeVals[0] is "time"
    const hoverTimeFound = reduce(
      sortedTimeVals,
      (acc, currentTime, index) => {
        const thisDiff = Math.abs(Number(hoverTime) - currentTime)
        if (thisDiff < acc.diff) {
          return {index, diff: thisDiff}
        }
        return acc
      },
      {index: 1, diff: firstDiff}
    )

    const {verticalTimeAxis} = tableOptions
    const scrollToColumn = verticalTimeAxis ? null : hoverTimeFound.index
    const scrollToRow = verticalTimeAxis ? hoverTimeFound.index : null
    return {scrollToRow, scrollToColumn}
  }

  private handleHover = (
    columnIndex: number,
    rowIndex: number
  ): (() => void) => (): void => {
    const {
      handleSetHoverTime,
      tableOptions: {verticalTimeAxis},
    } = this.props
    const {sortedTimeVals, isTimeVisible} = this.state
    if (verticalTimeAxis && rowIndex === 0) {
      return
    }
    if (handleSetHoverTime && isTimeVisible) {
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

  private handleMouseLeave = (): void => {
    if (this.props.handleSetHoverTime) {
      this.props.handleSetHoverTime(NULL_HOVER_TIME)
      this.setState({
        hoveredColumnIndex: NULL_ARRAY_INDEX,
        hoveredRowIndex: NULL_ARRAY_INDEX,
      })
    }
  }

  private handleClickFieldName = (clickedFieldName: string) => (): void => {
    const {tableOptions, fieldOptions, timeFormat, decimalPlaces} = this.props
    const {data, sort} = this.state

    if (clickedFieldName === sort.field) {
      sort.direction = sort.direction === ASCENDING ? DESCENDING : ASCENDING
    } else {
      sort.field = clickedFieldName
      sort.direction = DEFAULT_SORT_DIRECTION
    }

    const {transformedData, sortedTimeVals} = transformTableData(
      data,
      sort,
      fieldOptions,
      tableOptions,
      timeFormat,
      decimalPlaces
    )

    this.setState({
      transformedData,
      sortedTimeVals,
      sort,
    })
  }

  private calculateColumnWidth = (columnSizerWidth: number) => (column: {
    index: number
  }): number => {
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

  private createCellContents = (
    cellData: DbData,
    fieldName: DbData,
    isTimeData: boolean,
    isFieldName: boolean
  ): string => {
    const {timeFormat, decimalPlaces} = this.props
    if (isTimeData) {
      return `${moment(cellData).format(timeFormat)}`
    }
    if (typeof cellData === 'string' && isFieldName) {
      return `${fieldName}`
    }
    if (typeof cellData === 'number' && decimalPlaces.isEnforced) {
      return cellData.toFixed(decimalPlaces.digits)
    }
    return `${cellData}`
  }

  private cellRenderer = ({columnIndex, rowIndex, key, parent, style}) => {
    const {
      hoveredColumnIndex,
      hoveredRowIndex,
      transformedData,
      sort,
      isTimeVisible,
    } = this.state

    const {
      fieldOptions = [DEFAULT_TIME_FIELD],
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

    const isFixedRow = rowIndex === 0 && columnIndex > 0
    const isFixedColumn = fixFirstColumn && rowIndex > 0 && columnIndex === 0
    const isTimeData =
      isTimeVisible &&
      (verticalTimeAxis
        ? rowIndex !== 0 && columnIndex === timeFieldIndex
        : rowIndex === timeFieldIndex && columnIndex !== 0)
    const isFieldName = verticalTimeAxis ? rowIndex === 0 : columnIndex === 0
    const isFixedCorner = rowIndex === 0 && columnIndex === 0
    const isNumerical = _.isNumber(cellData)

    const isHighlightedRow =
      rowIndex === parent.props.scrollToRow ||
      (rowIndex === hoveredRowIndex && hoveredRowIndex !== 0)
    const isHighlightedColumn =
      columnIndex === parent.props.scrollToColumn ||
      (columnIndex === hoveredColumnIndex && hoveredColumnIndex !== 0)

    let cellStyle = style

    if (
      !isFixedRow &&
      !isFixedColumn &&
      !isFixedCorner &&
      !isTimeData &&
      isNumerical
    ) {
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
      'table-graph-cell__numerical': isNumerical,
      'table-graph-cell__field-name': isFieldName,
      'table-graph-cell__sort-asc':
        isFieldName && sort.field === cellData && sort.direction === ASCENDING,
      'table-graph-cell__sort-desc':
        isFieldName && sort.field === cellData && sort.direction === DESCENDING,
    })

    const cellContents = this.createCellContents(
      cellData,
      fieldName,
      isTimeData,
      isFieldName
    )

    return (
      <div
        key={key}
        style={cellStyle}
        className={cellClass}
        onClick={
          isFieldName && typeof cellData === 'string'
            ? this.handleClickFieldName(cellData)
            : null
        }
        onMouseOver={_.throttle(this.handleHover(columnIndex, rowIndex), 100)}
        title={cellContents}
      >
        {cellContents}
      </div>
    )
  }

  private getMultiGridRef = (r, registerChild) => {
    return registerChild(r)
  }
}

const mapDispatchToProps = dispatch => ({
  handleUpdateFieldOptions: bindActionCreators(updateFieldOptions, dispatch),
})

export default connect(null, mapDispatchToProps)(TableGraph)
