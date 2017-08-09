import React, {PropTypes} from 'react'

import Dimensions from 'react-dimensions'
import _ from 'lodash'
import moment from 'moment'
import classnames from 'classnames'

import Dropdown from 'shared/components/Dropdown'
import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'

import {Table, Column, Cell} from 'fixed-data-table'

const {arrayOf, bool, func, number, oneOfType, shape, string} = PropTypes

const defaultTableHeight = 1000
const emptySeries = {columns: [], values: []}

const CustomCell = React.createClass({
  propTypes: {
    data: oneOfType([number, string]),
    columnName: string.isRequired,
  },

  render() {
    const {columnName, data} = this.props

    if (columnName === 'time') {
      const date = moment(new Date(data)).format('MM/DD/YY hh:mm:ssA')

      return (
        <span>
          {date}
        </span>
      )
    }

    return (
      <span>
        {data}
      </span>
    )
  },
})

const ChronoTable = React.createClass({
  propTypes: {
    query: shape({
      host: arrayOf(string.isRequired).isRequired,
      text: string.isRequired,
      id: string.isRequired,
    }).isRequired,
    containerWidth: number.isRequired,
    height: number,
    editQueryStatus: func.isRequired,
  },

  getInitialState() {
    return {
      series: [emptySeries],
      columnWidths: {},
      activeSeriesIndex: 0,
    }
  },

  getDefaultProps() {
    return {
      height: defaultTableHeight,
    }
  },

  componentDidMount() {
    this.fetchCellData(this.props.query)
  },

  componentWillReceiveProps(nextProps) {
    if (this.props.query.text === nextProps.query.text) {
      return
    }

    this.fetchCellData(nextProps.query)
  },

  async fetchCellData(query) {
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
  },

  handleColumnResize(newColumnWidth, columnKey) {
    const columnWidths = {
      ...this.state.columnWidths,
      [columnKey]: newColumnWidth,
    }

    this.setState({
      columnWidths,
    })
  },

  handleClickTab(activeSeriesIndex) {
    this.setState({activeSeriesIndex})
  },

  handleClickDropdown(item) {
    this.setState({activeSeriesIndex: item.index})
  },

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
    const stylePixelOffset = 136
    const rowHeight = 34
    const defaultColumnWidth = 200
    const headerHeight = 30
    const minWidth = 70
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
              {series.map(({name}, i) =>
                <TabItem
                  isActive={i === activeSeriesIndex}
                  key={i}
                  name={name}
                  index={i}
                  onClickTab={this.handleClickTab}
                />
              )}
            </div>
          : <Dropdown
              className="dropdown-160 table--tabs-dropdown"
              items={series.map((s, index) => ({...s, text: s.name, index}))}
              onChoose={this.handleClickDropdown}
              selected={series[activeSeriesIndex].name}
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
                      cell={({rowIndex}) =>
                        <CustomCell
                          columnName={columnName}
                          data={values[rowIndex][colIndex]}
                        />}
                      width={columnWidths[columnName] || width}
                      minWidth={minWidth}
                    />
                  )
                })}
              </Table>}
        </div>
      </div>
    )
  },
})

const TabItem = ({name, index, onClickTab, isActive}) =>
  <div
    className={classnames('table--tab', {active: isActive})}
    onClick={() => onClickTab(index)}
  >
    {name}
  </div>

TabItem.propTypes = {
  name: string,
  onClickTab: func.isRequired,
  index: number.isRequired,
  isActive: bool,
}

export default Dimensions({elementResize: true})(ChronoTable)
