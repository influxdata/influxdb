// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import TableCell from 'src/shared/components/tables/TableCell'
import {ColumnSizer, SizedColumnProps, AutoSizer} from 'react-virtualized'
import {MultiGrid, PropsMultiGrid} from 'src/shared/components/MultiGrid'
import InvalidData from 'src/shared/components/InvalidData'
import {fastReduce} from 'src/utils/fast'

// Actions
import {setHoverTime as setHoverTimeAction} from 'src/dashboards/actions/v2/hoverTime'

// Utils
import {transformTableData} from 'src/dashboards/utils/tableGraph'

// Constants
import {
  ASCENDING,
  DESCENDING,
  NULL_HOVER_TIME,
  NULL_ARRAY_INDEX,
  DEFAULT_FIX_FIRST_COLUMN,
  DEFAULT_VERTICAL_TIME_AXIS,
  DEFAULT_SORT_DIRECTION,
} from 'src/shared/constants/tableGraph'
import {DEFAULT_TIME_FIELD} from 'src/dashboards/constants'
const COLUMN_MIN_WIDTH = 100
const ROW_HEIGHT = 30

import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Sort} from 'src/types/v2/dashboards'
import {TableView} from 'src/types/v2/dashboards'
import {TimeSeriesValue} from 'src/types/series'

export interface CellRendererProps {
  columnIndex: number
  rowIndex: number
  key: string
  parent: React.Component<PropsMultiGrid>
  style: React.CSSProperties
}

export interface Label {
  label: string
  seriesIndex: number
  responseIndex: number
}

enum ErrorTypes {
  MetaQueryCombo = 'MetaQueryCombo',
  GeneralError = 'Error',
}

interface Props {
  data: TimeSeriesValue[][]
  sortedLabels: Label[]
  properties: TableView
  hoverTime: string
  onSetHoverTime?: (hovertime: string) => void
}

interface State {
  transformedData: TimeSeriesValue[][]
  sortedTimeVals: TimeSeriesValue[]
  sortedLabels: Label[]
  hoveredColumnIndex: number
  hoveredRowIndex: number
  timeColumnWidth: number
  sort: Sort
  columnWidths: {[x: string]: number}
  totalColumnWidths: number
  isTimeVisible: boolean
  shouldResize: boolean
  invalidDataError: ErrorTypes
}

@ErrorHandling
class TableGraph extends PureComponent<Props, State> {
  private gridContainer: HTMLDivElement
  private multiGrid?: MultiGrid

  constructor(props: Props) {
    super(props)

    const sortField: string = _.get(
      this.props,
      'tableOptions.sortBy.internalName',
      ''
    )

    const {data, sortedLabels} = this.props

    this.state = {
      sortedLabels,
      columnWidths: {},
      timeColumnWidth: 0,
      sortedTimeVals: [],
      shouldResize: false,
      isTimeVisible: true,
      totalColumnWidths: 0,
      transformedData: data,
      invalidDataError: null,
      hoveredRowIndex: NULL_ARRAY_INDEX,
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      sort: {field: sortField, direction: DEFAULT_SORT_DIRECTION},
    }
  }

  public render() {
    const {transformedData} = this.state

    const rowCount = this.columnCount === 0 ? 0 : transformedData.length
    const fixedColumnCount = this.fixFirstColumn && this.columnCount > 1 ? 1 : 0
    const {scrollToColumn, scrollToRow} = this.scrollToColRow

    if (this.state.invalidDataError) {
      return <InvalidData />
    }

    return (
      <div
        className="time-machine-table"
        ref={gridContainer => (this.gridContainer = gridContainer)}
        onMouseLeave={this.handleMouseLeave}
      >
        {rowCount > 0 && (
          <AutoSizer>
            {({width, height}) => {
              return (
                <ColumnSizer
                  columnCount={this.computedColumnCount}
                  columnMinWidth={COLUMN_MIN_WIDTH}
                  width={width}
                >
                  {({
                    adjustedWidth,
                    columnWidth,
                    registerChild,
                  }: SizedColumnProps) => {
                    return (
                      <MultiGrid
                        height={height}
                        ref={registerChild}
                        rowCount={rowCount}
                        width={adjustedWidth}
                        rowHeight={ROW_HEIGHT}
                        scrollToRow={scrollToRow}
                        columnCount={this.columnCount}
                        scrollToColumn={scrollToColumn}
                        fixedColumnCount={fixedColumnCount}
                        cellRenderer={this.cellRenderer}
                        onMount={this.handleMultiGridMount}
                        classNameBottomRightGrid="table-graph--scroll-window"
                        columnWidth={this.calculateColumnWidth(columnWidth)}
                      />
                    )
                  }}
                </ColumnSizer>
              )
            }}
          </AutoSizer>
        )}
      </div>
    )
  }

  public async componentDidMount() {
    const {properties} = this.props
    const {fieldOptions} = properties

    window.addEventListener('resize', this.handleResize)

    let sortField: string = _.get(
      properties,
      ['tableOptions', 'sortBy', 'internalName'],
      ''
    )
    const isValidSortField = !!fieldOptions.find(
      f => f.internalName === sortField
    )

    if (!isValidSortField) {
      sortField = _.get(
        DEFAULT_TIME_FIELD,
        'internalName',
        _.get(fieldOptions, '0.internalName', '')
      )
    }

    const sort: Sort = {field: sortField, direction: DEFAULT_SORT_DIRECTION}

    try {
      const isTimeVisible = _.get(this.timeField, 'visible', false)

      this.setState(
        {
          hoveredColumnIndex: NULL_ARRAY_INDEX,
          hoveredRowIndex: NULL_ARRAY_INDEX,
          sort,
          isTimeVisible,
          invalidDataError: null,
        },
        () => {
          window.setTimeout(() => {
            this.forceUpdate()
          }, 0)
        }
      )
    } catch (e) {
      this.handleError(e)
    }
  }

  public componentDidUpdate() {
    if (this.state.shouldResize) {
      if (this.multiGrid) {
        this.multiGrid.recomputeGridSize()
      }

      this.setState({shouldResize: false})
    }
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.handleResize)
  }

  private handleMultiGridMount = (ref: MultiGrid) => {
    this.multiGrid = ref
    ref.forceUpdate()
  }

  private handleError(e: Error): void {
    let invalidDataError: ErrorTypes
    switch (e.toString()) {
      case 'Error: Cannot display meta and data query':
        invalidDataError = ErrorTypes.MetaQueryCombo
        break
      default:
        invalidDataError = ErrorTypes.GeneralError
        break
    }
    this.setState({invalidDataError})
  }

  public get timeField() {
    const {fieldOptions} = this.props.properties

    return _.find(
      fieldOptions,
      f => f.internalName === DEFAULT_TIME_FIELD.internalName
    )
  }

  private get fixFirstColumn(): boolean {
    const {tableOptions, fieldOptions} = this.props.properties
    const {fixFirstColumn = DEFAULT_FIX_FIRST_COLUMN} = tableOptions

    if (fieldOptions.length === 1) {
      return false
    }

    const visibleFields = fieldOptions.reduce((acc, f) => {
      if (f.visible) {
        acc += 1
      }
      return acc
    }, 0)

    if (visibleFields === 1) {
      return false
    }

    return fixFirstColumn
  }

  private get columnCount(): number {
    const {transformedData} = this.state
    return _.get(transformedData, ['0', 'length'], 0)
  }

  private get computedColumnCount(): number {
    if (this.fixFirstColumn) {
      return this.columnCount - 1
    }

    return this.columnCount
  }

  private get tableWidth(): number {
    let tableWidth = 0

    if (this.gridContainer && this.gridContainer.clientWidth) {
      tableWidth = this.gridContainer.clientWidth
    }

    return tableWidth
  }

  private get isEmpty(): boolean {
    const {data} = this.props
    return _.isEmpty(data[0])
  }

  private get scrollToColRow(): {
    scrollToRow: number | null
    scrollToColumn: number | null
  } {
    const {sortedTimeVals, hoveredColumnIndex, isTimeVisible} = this.state
    const {hoverTime} = this.props
    const hoveringThisTable = hoveredColumnIndex !== NULL_ARRAY_INDEX
    const notHovering = hoverTime === NULL_HOVER_TIME
    if (this.isEmpty || notHovering || hoveringThisTable || !isTimeVisible) {
      return {scrollToColumn: 0, scrollToRow: -1}
    }

    const firstDiff = this.getTimeDifference(hoverTime, sortedTimeVals[1]) // sortedTimeVals[0] is "time"
    const hoverTimeFound = fastReduce<
      TimeSeriesValue,
      {index: number; diff: number}
    >(
      sortedTimeVals,
      (acc, currentTime, index) => {
        const thisDiff = this.getTimeDifference(hoverTime, currentTime)
        if (thisDiff < acc.diff) {
          return {index, diff: thisDiff}
        }
        return acc
      },
      {index: 1, diff: firstDiff}
    )

    const scrollToColumn = this.isVerticalTimeAxis ? -1 : hoverTimeFound.index
    const scrollToRow = this.isVerticalTimeAxis ? hoverTimeFound.index : null
    return {scrollToRow, scrollToColumn}
  }

  private getTimeDifference(hoverTime, time: string | number) {
    return Math.abs(parseInt(hoverTime, 10) - parseInt(time as string, 10))
  }

  private get isVerticalTimeAxis(): boolean {
    return _.get(
      this.props.properties,
      'tableOptions.verticalTimeAxis',
      DEFAULT_VERTICAL_TIME_AXIS
    )
  }

  private handleHover = (e: React.MouseEvent<HTMLElement>) => {
    const {dataset} = e.target as HTMLElement
    const {onSetHoverTime} = this.props
    const {sortedTimeVals, isTimeVisible} = this.state
    if (this.isVerticalTimeAxis && +dataset.rowIndex === 0) {
      return
    }
    if (onSetHoverTime && isTimeVisible) {
      const hoverTime = this.isVerticalTimeAxis
        ? sortedTimeVals[dataset.rowIndex]
        : sortedTimeVals[dataset.columnIndex]
      onSetHoverTime(_.defaultTo(hoverTime, '').toString())
    }
    this.setState({
      hoveredColumnIndex: +dataset.columnIndex,
      hoveredRowIndex: +dataset.rowIndex,
    })
  }

  private handleMouseLeave = (): void => {
    if (this.props.onSetHoverTime) {
      this.props.onSetHoverTime(NULL_HOVER_TIME)
      this.setState({
        hoveredColumnIndex: NULL_ARRAY_INDEX,
        hoveredRowIndex: NULL_ARRAY_INDEX,
      })
    }
  }

  private handleClickFieldName = (
    clickedFieldName: string
  ) => async (): Promise<void> => {
    const {data, properties} = this.props
    const {tableOptions, fieldOptions, timeFormat, decimalPlaces} = properties
    const {sort} = this.state

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

    const {transformedData, columnWidths, totalColumnWidths} = this.state
    const columnLabel = transformedData[0][index]

    const original = columnWidths[columnLabel] || 0

    if (this.fixFirstColumn && index === 0) {
      return original
    }

    if (this.tableWidth <= totalColumnWidths) {
      return original
    }

    if (this.columnCount <= 1) {
      return columnSizerWidth
    }

    const difference = this.tableWidth - totalColumnWidths
    const increment = difference / this.computedColumnCount

    return original + increment
  }

  private handleResize = () => {
    this.forceUpdate()
  }

  private getCellData = (rowIndex, columnIndex) => {
    return this.state.transformedData[rowIndex][columnIndex]
  }

  private cellRenderer = (cellProps: CellRendererProps) => {
    const {rowIndex, columnIndex} = cellProps
    const {
      sort,
      isTimeVisible,
      hoveredRowIndex,
      hoveredColumnIndex,
    } = this.state

    return (
      <TableCell
        {...cellProps}
        sort={sort}
        onHover={this.handleHover}
        isTimeVisible={isTimeVisible}
        data={this.getCellData(rowIndex, columnIndex)}
        hoveredRowIndex={hoveredRowIndex}
        properties={this.props.properties}
        hoveredColumnIndex={hoveredColumnIndex}
        isFirstColumnFixed={this.fixFirstColumn}
        isVerticalTimeAxis={this.isVerticalTimeAxis}
        onClickFieldName={this.handleClickFieldName}
      />
    )
  }
}

const mstp = ({hoverTime}) => ({
  hoverTime,
})

const mdtp = {
  onSetHoverTime: setHoverTimeAction,
}

export default connect(mstp, mdtp)(TableGraph)
