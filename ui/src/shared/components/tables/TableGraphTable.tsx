// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {timeFormatter} from '@influxdata/giraffe'
import classnames from 'classnames'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import TableCell from 'src/shared/components/tables/TableCell'
import {ColumnSizer, SizedColumnProps, AutoSizer} from 'react-virtualized'
import {MultiGrid, PropsMultiGrid} from 'src/shared/components/MultiGrid'

// Utils
import {withHoverTime, InjectedHoverProps} from 'src/dashboards/utils/hoverTime'
import {
  findHoverTimeIndex,
  resolveTimeFormat,
} from 'src/dashboards/utils/tableGraph'

// Constants
import {
  NULL_ARRAY_INDEX,
  DEFAULT_FIX_FIRST_COLUMN,
  DEFAULT_VERTICAL_TIME_AXIS,
} from 'src/shared/constants/tableGraph'
import {DEFAULT_TIME_FIELD} from 'src/dashboards/constants'
const COLUMN_MIN_WIDTH = 100
const ROW_HEIGHT = 30

// Types
import {TableViewProperties, TimeZone, Theme} from 'src/types'
import {TransformTableDataReturnType} from 'src/dashboards/utils/tableGraph'

export interface ColumnWidths {
  totalWidths: number
  widths: {[x: string]: number}
}

export interface CellRendererProps {
  columnIndex: number
  rowIndex: number
  key: string
  parent: React.Component<PropsMultiGrid>
  style: React.CSSProperties
}

interface OwnProps {
  dataTypes: {[x: string]: string}
  transformedDataBundle: TransformTableDataReturnType
  properties: TableViewProperties
  onSort: (fieldName: string) => void
  timeZone: TimeZone
  theme: Theme
}

type Props = OwnProps & InjectedHoverProps

interface State {
  timeColumnWidth: number
  hoveredColumnIndex: number
  hoveredRowIndex: number
  totalColumnWidths: number
  shouldResize: boolean
}

@ErrorHandling
class TableGraphTable extends PureComponent<Props, State> {
  public state = {
    timeColumnWidth: 0,
    shouldResize: false,
    totalColumnWidths: 0,
    hoveredRowIndex: NULL_ARRAY_INDEX,
    hoveredColumnIndex: NULL_ARRAY_INDEX,
  }

  private gridContainer: HTMLDivElement
  private multiGrid?: MultiGrid

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

  public render() {
    const {
      transformedDataBundle: {transformedData},
      theme,
    } = this.props

    const rowCount = this.columnCount === 0 ? 0 : transformedData.length
    const fixedColumnCount = this.fixFirstColumn && this.columnCount > 1 ? 1 : 0
    const {scrollToColumn, scrollToRow} = this.scrollToColRow
    const tableClassName = classnames('time-machine-table', {
      'time-machine-table__light-mode': theme === 'light',
    })

    return (
      <div
        className={tableClassName}
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

  private get timeField() {
    const {transformedDataBundle} = this.props
    const {resolvedFieldOptions} = transformedDataBundle

    return _.find(
      resolvedFieldOptions,
      f => f.internalName === DEFAULT_TIME_FIELD.internalName
    )
  }

  private get fixFirstColumn(): boolean {
    const {
      transformedDataBundle: {resolvedFieldOptions},
      properties: {tableOptions},
    } = this.props

    const {fixFirstColumn = DEFAULT_FIX_FIRST_COLUMN} = tableOptions

    if (resolvedFieldOptions.length === 1) {
      return false
    }

    const visibleFields = resolvedFieldOptions.reduce((acc, f) => {
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
    const {
      transformedDataBundle: {transformedData},
    } = this.props
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

  private get scrollToColRow(): {
    scrollToRow: number | null
    scrollToColumn: number | null
  } {
    const {
      transformedDataBundle: {sortedTimeVals},
    } = this.props
    const {hoveredColumnIndex} = this.state
    const {hoverTime} = this.props
    const hoveringThisTable = hoveredColumnIndex !== NULL_ARRAY_INDEX

    if (!hoverTime || hoveringThisTable || !this.isTimeVisible) {
      return {scrollToColumn: 0, scrollToRow: -1}
    }

    const hoverIndex = findHoverTimeIndex(sortedTimeVals, hoverTime)
    const scrollToColumn = this.isVerticalTimeAxis ? -1 : hoverIndex
    const scrollToRow = this.isVerticalTimeAxis ? hoverIndex : null
    return {scrollToRow, scrollToColumn}
  }

  private get isVerticalTimeAxis(): boolean {
    const {
      properties: {tableOptions},
    } = this.props

    const {verticalTimeAxis = DEFAULT_VERTICAL_TIME_AXIS} = tableOptions
    return verticalTimeAxis
  }

  private get isTimeVisible(): boolean {
    return _.get(this.timeField, 'visible', false)
  }

  private handleMultiGridMount = (ref: MultiGrid) => {
    this.multiGrid = ref
    ref.forceUpdate()
  }

  private handleHover = (e: React.MouseEvent<HTMLElement>) => {
    const {dataset} = e.target as HTMLElement
    const {onSetHoverTime} = this.props
    const {
      transformedDataBundle: {sortedTimeVals},
    } = this.props

    if (this.isVerticalTimeAxis && +dataset.rowIndex === 0) {
      return
    }
    if (onSetHoverTime && this.isTimeVisible) {
      const hoverTime = this.isVerticalTimeAxis
        ? sortedTimeVals[dataset.rowIndex]
        : sortedTimeVals[dataset.columnIndex]

      onSetHoverTime(new Date(hoverTime).valueOf())
    }
    this.setState({
      hoveredColumnIndex: +dataset.columnIndex,
      hoveredRowIndex: +dataset.rowIndex,
    })
  }

  private handleMouseLeave = (): void => {
    const {onSetHoverTime} = this.props

    if (onSetHoverTime) {
      onSetHoverTime(0)
    }
    this.setState({
      hoveredColumnIndex: NULL_ARRAY_INDEX,
      hoveredRowIndex: NULL_ARRAY_INDEX,
    })
  }

  private calculateColumnWidth = (columnSizerWidth: number) => (column: {
    index: number
  }): number => {
    const {index} = column

    const {
      transformedDataBundle: {transformedData, columnWidths},
    } = this.props

    const {totalColumnWidths} = this.state
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
    const {
      transformedDataBundle: {transformedData},
    } = this.props
    return transformedData[rowIndex][columnIndex]
  }

  private dataType = (rowIndex, columnIndex): string => {
    const {
      transformedDataBundle: {transformedData},
      dataTypes,
    } = this.props

    if (rowIndex === 0) {
      return 'n/a'
    }

    const columnName = transformedData[0][columnIndex]

    return _.get(dataTypes, columnName, 'n/a')
  }

  private get timeFormatter() {
    const {
      timeZone,
      properties: {timeFormat},
    } = this.props

    return timeFormatter({
      timeZone: timeZone === 'Local' ? undefined : timeZone,
      format: resolveTimeFormat(timeFormat),
    })
  }

  private cellRenderer = (cellProps: CellRendererProps) => {
    const {rowIndex, columnIndex} = cellProps
    const {
      transformedDataBundle: {sortOptions, resolvedFieldOptions},
      onSort,
      properties,
    } = this.props
    const {hoveredRowIndex, hoveredColumnIndex} = this.state
    const {scrollToRow} = this.scrollToColRow
    const hoverIndex = scrollToRow >= 0 ? scrollToRow : hoveredRowIndex

    return (
      <TableCell
        {...cellProps}
        sortOptions={sortOptions}
        onHover={this.handleHover}
        isTimeVisible={this.isTimeVisible}
        data={this.getCellData(rowIndex, columnIndex)}
        dataType={this.dataType(rowIndex, columnIndex)}
        hoveredRowIndex={hoverIndex}
        properties={properties}
        resolvedFieldOptions={resolvedFieldOptions}
        hoveredColumnIndex={hoveredColumnIndex}
        isFirstColumnFixed={this.fixFirstColumn}
        isVerticalTimeAxis={this.isVerticalTimeAxis}
        onClickFieldName={onSort}
        timeFormatter={this.timeFormatter}
      />
    )
  }
}

export default withHoverTime(TableGraphTable)
