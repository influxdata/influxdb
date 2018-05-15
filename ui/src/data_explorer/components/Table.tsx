import React, {PureComponent, CSSProperties} from 'react'

import Dimensions from 'react-dimensions'
import _ from 'lodash'

import {Table, Column, Cell} from 'fixed-data-table'
import Dropdown from 'src/shared/components/Dropdown'
import CustomCell from 'src/data_explorer/components/CustomCell'
import TabItem from 'src/data_explorer/components/TableTabItem'
import {TEMPLATES} from 'src/shared/constants'

import {fetchTimeSeriesAsync} from 'src/shared/actions/timeSeries'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  emptySeries,
  maximumTabsCount,
  minWidth,
  rowHeight,
  headerHeight,
  stylePixelOffset,
  defaultColumnWidth,
} from 'src/data_explorer/constants/table'

interface DataExplorerTableQuery {
  host: string[]
  text: string
  id: string
}

interface Series {
  columns: string[]
  name: string
  values: any[]
}

interface ColumnWidths {
  [key: string]: number
}

interface Props {
  height: number
  query: DataExplorerTableQuery
  editQueryStatus: () => void
  containerHeight: number
  containerWidth: number
}

interface State {
  series: Series[]
  columnWidths: ColumnWidths
  activeSeriesIndex: number
  isLoading: boolean
}

@ErrorHandling
class ChronoTable extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      series: [emptySeries],
      columnWidths: {},
      activeSeriesIndex: 0,
      isLoading: false,
    }
  }

  public componentDidMount() {
    this.fetchCellData(this.props.query)
  }

  public componentWillReceiveProps(nextProps) {
    if (this.props.query.text === nextProps.query.text) {
      return
    }

    this.fetchCellData(nextProps.query)
  }

  public render() {
    const {query, containerWidth} = this.props
    const {series, columnWidths, isLoading, activeSeriesIndex} = this.state
    const {columns, values} = _.get(series, `${activeSeriesIndex}`, emptySeries)

    if (!query) {
      return (
        <div className="generic-empty-state"> Please add a query below </div>
      )
    }

    if (isLoading) {
      return <div className="generic-empty-state"> Loading... </div>
    }

    return (
      <div style={this.style}>
        {this.tableSelector}
        <div className="table--tabs-content">
          {this.isEmpty ? (
            <div className="generic-empty-state"> This series is empty </div>
          ) : (
            <Table
              isColumnResizing={false}
              width={containerWidth}
              rowHeight={rowHeight}
              height={this.height}
              ownerHeight={this.height}
              rowsCount={values.length}
              headerHeight={headerHeight}
              onColumnResizeEndCallback={this.handleColumnResize}
            >
              {columns.map((columnName, colIndex) => {
                return (
                  <Column
                    isResizable={true}
                    key={columnName}
                    minWidth={minWidth}
                    columnKey={columnName}
                    header={<Cell> {columnName} </Cell>}
                    width={columnWidths[columnName] || this.columnWidth}
                    cell={this.handleCustomCell(columnName, values, colIndex)}
                  />
                )
              })}
            </Table>
          )}
        </div>
      </div>
    )
  }

  private get isEmpty(): boolean {
    const {columns, values} = this.series
    return (columns && !columns.length) || (values && !values.length)
  }

  private get height(): number {
    return this.props.containerHeight || 500 - stylePixelOffset
  }

  private get tableSelector() {
    if (this.isTabbed) {
      return this.tabs
    }

    return this.dropdown
  }

  private get dropdown(): JSX.Element {
    const {series, activeSeriesIndex} = this.state
    return (
      <Dropdown
        className="dropdown-160 table--tabs-dropdown"
        items={this.dropdownItems}
        onChoose={this.handleClickDropdown}
        selected={this.makeTabName(series[activeSeriesIndex])}
        buttonSize="btn-xs"
      />
    )
  }

  private get dropdownItems(): Series[] {
    return this.state.series.map((s, index) => ({
      ...s,
      index,
      text: this.makeTabName(s),
    }))
  }

  private get tabs(): JSX.Element {
    const {series, activeSeriesIndex} = this.state
    return (
      <div className="table--tabs">
        {series.map((s, i) => (
          <TabItem
            key={i}
            index={i}
            name={s.name}
            onClickTab={this.handleClickTab}
            isActive={i === activeSeriesIndex}
          />
        ))}
      </div>
    )
  }

  private isTabbed(): boolean {
    const {series} = this.state

    return series.length < maximumTabsCount
  }

  private get style(): CSSProperties {
    return {
      width: '100%',
      height: '100%',
      position: 'relative',
    }
  }

  private get columnWidth(): number {
    return defaultColumnWidth
  }

  private get series(): Series {
    const {activeSeriesIndex} = this.state
    const {series} = this.state

    return _.get(series, `${activeSeriesIndex}`, emptySeries)
  }

  private get source(): string {
    return _.get(this.props.query, 'host.0', '')
  }

  private fetchCellData = async (query: DataExplorerTableQuery) => {
    if (!query || !query.text) {
      return
    }

    this.setState({
      isLoading: true,
    })

    try {
      const {results} = await fetchTimeSeriesAsync({
        source: this.source,
        query,
        tempVars: TEMPLATES,
      })

      this.setState({
        isLoading: false,
      })

      let series = _.get(results, ['0', 'series'], [])

      if (!series.length) {
        return this.setState({series: []})
      }

      series = series.map(s => {
        if (s.values) {
          return s
        }

        return {...s, values: []}
      })

      this.setState({series})
    } catch (error) {
      this.setState({
        isLoading: false,
        series: [],
      })
      throw error
    }
  }

  private handleColumnResize = (
    newColumnWidth: number,
    columnKey: string
  ): void => {
    const columnWidths = {
      ...this.state.columnWidths,
      [columnKey]: newColumnWidth,
    }

    this.setState({
      columnWidths,
    })
  }

  private handleClickTab = activeSeriesIndex => {
    this.setState({
      activeSeriesIndex,
    })
  }

  private handleClickDropdown = item => {
    this.setState({
      activeSeriesIndex: item.index,
    })
  }

  private handleCustomCell = (columnName, values, colIndex) => ({rowIndex}) => {
    return (
      <CustomCell columnName={columnName} data={values[rowIndex][colIndex]} />
    )
  }

  private makeTabName = ({name}): string => {
    return name
  }
}

export default Dimensions({
  elementResize: true,
})(ChronoTable)
