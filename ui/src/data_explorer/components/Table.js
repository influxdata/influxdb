import React, {PropTypes, Component} from 'react'

import Dimensions from 'react-dimensions'
import _ from 'lodash'

import {Table, Column, Cell} from 'fixed-data-table'
import Dropdown from 'shared/components/Dropdown'
import CustomCell from 'src/data_explorer/components/CustomCell'
import TabItem from 'src/data_explorer/components/TableTabItem'

import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'

const emptySeries = {columns: [], values: []}

class ChronoTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      series: [emptySeries],
      columnWidths: {},
      activeSeriesIndex: 0,
    }
  }

  componentDidMount() {
    this.fetchCellData(this.props.query)
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.query.text === nextProps.query.text) {
      return
    }

    this.fetchCellData(nextProps.query)
  }

  fetchCellData = async query => {
    if (!query || !query.text) {
      return
    }

    this.setState({isLoading: true})
    // second param is db, we want to leave this blank
    try {
      const {results} = await fetchTimeSeriesAsync({source: query.host, query})
      this.setState({isLoading: false})

      let series = _.get(results, ['0', 'series'], [])

      if (!series.length) {
        return this.setState({series: []})
      }

      series = series.map(s => (s.values ? s : {...s, values: []}))
      this.setState({series})
    } catch (error) {
      this.setState({
        isLoading: false,
        series: [],
      })
      throw error
    }
  }

  handleColumnResize = (newColumnWidth, columnKey) => {
    const columnWidths = {
      ...this.state.columnWidths,
      [columnKey]: newColumnWidth,
    }

    this.setState({
      columnWidths,
    })
  }

  handleClickTab = activeSeriesIndex => () => {
    this.setState({activeSeriesIndex})
  }

  handleClickDropdown = item => {
    this.setState({activeSeriesIndex: item.index})
  }

  handleCustomCell = (columnName, values, colIndex) => ({rowIndex}) => {
    return (
      <CustomCell columnName={columnName} data={values[rowIndex][colIndex]} />
    )
  }

  makeTabName = ({name, tags}) => {
    if (!tags) {
      return name
    }
    const tagKeys = Object.keys(tags).sort()
    const tagValues = tagKeys.map(key => tags[key]).join('.')
    return `${name}.${tagValues}`
  }

  render() {
    const {containerWidth, height, query} = this.props
    const {series, columnWidths, isLoading, activeSeriesIndex} = this.state
    const {columns, values} = _.get(
      series,
      [`${activeSeriesIndex}`],
      emptySeries
    )

    const maximumTabsCount = 11
    // adjust height to proper value by subtracting the heights of the UI around it
    // tab height, graph-container vertical padding, graph-heading height, multitable-header height
    const minWidth = 70
    const rowHeight = 34
    const headerHeight = 30
    const stylePixelOffset = 130
    const defaultColumnWidth = 200
    const styleAdjustedHeight = height - stylePixelOffset
    const width =
      columns && columns.length > 1 ? defaultColumnWidth : containerWidth

    if (!query) {
      return <div className="generic-empty-state">Please add a query below</div>
    }

    if (isLoading) {
      return <div className="generic-empty-state">Loading...</div>
    }

    return (
      <div style={{width: '100%', height: '100%', position: 'relative'}}>
        {series.length < maximumTabsCount
          ? <div className="table--tabs">
              {series.map((s, i) =>
                <TabItem
                  isActive={i === activeSeriesIndex}
                  key={i}
                  name={this.makeTabName(s)}
                  index={i}
                  onClickTab={this.handleClickTab}
                />
              )}
            </div>
          : <Dropdown
              className="dropdown-160 table--tabs-dropdown"
              items={series.map((s, index) => ({
                ...s,
                text: this.makeTabName(s),
                index,
              }))}
              onChoose={this.handleClickDropdown}
              selected={this.makeTabName(series[activeSeriesIndex])}
              buttonSize="btn-xs"
            />}
        <div className="table--tabs-content">
          {(columns && !columns.length) || (values && !values.length)
            ? <div className="generic-empty-state">This series is empty</div>
            : <Table
                onColumnResizeEndCallback={this.handleColumnResize}
                isColumnResizing={false}
                rowHeight={rowHeight}
                rowsCount={values.length}
                width={containerWidth}
                ownerHeight={styleAdjustedHeight}
                height={styleAdjustedHeight}
                headerHeight={headerHeight}
              >
                {columns.map((columnName, colIndex) => {
                  return (
                    <Column
                      isResizable={true}
                      key={columnName}
                      columnKey={columnName}
                      header={
                        <Cell>
                          {columnName}
                        </Cell>
                      }
                      cell={this.handleCustomCell(columnName, values, colIndex)}
                      width={columnWidths[columnName] || width}
                      minWidth={minWidth}
                    />
                  )
                })}
              </Table>}
        </div>
      </div>
    )
  }
}

ChronoTable.defaultProps = {
  height: 500,
}

const {arrayOf, func, number, shape, string} = PropTypes

ChronoTable.propTypes = {
  query: shape({
    host: arrayOf(string.isRequired).isRequired,
    text: string.isRequired,
    id: string.isRequired,
  }).isRequired,
  containerWidth: number.isRequired,
  height: number,
  editQueryStatus: func.isRequired,
}

export default Dimensions({elementResize: true})(ChronoTable)
