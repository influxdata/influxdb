import React, {PropTypes} from 'react'

import Dimensions from 'react-dimensions'
import _ from 'lodash'
import moment from 'moment'

import {fetchTimeSeriesAsync} from 'shared/actions/timeSeries'

import {Table, Column, Cell} from 'fixed-data-table'

const {arrayOf, func, number, oneOfType, shape, string} = PropTypes

const defaultTableHeight = 1000

const CustomCell = React.createClass({
  propTypes: {
    data: oneOfType([number, string]),
    columnName: string.isRequired,
  },

  render() {
    const {columnName, data} = this.props

    if (columnName === 'time') {
      const date = moment(new Date(data)).format('MM/DD/YY hh:mm:ssA')

      return <span>{date}</span>
    }

    return <span>{data}</span>
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
      series: [],
      columnWidths: {},
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
    this.setState(({columnWidths}) => ({
      columnWidths: Object.assign({}, columnWidths, {
        [columnKey]: newColumnWidth,
      }),
    }))
  },

  // Table data as a list of array.
  render() {
    const {containerWidth, height, query} = this.props
    const {series, columnWidths, isLoading} = this.state

    // adjust height to proper value by subtracting the heights of the UI around it
    // tab height, graph-container vertical padding, graph-heading height, multitable-header height
    const stylePixelOffset = 136
    const rowHeight = 34
    const defaultColumnWidth = 200
    const headerHeight = 30
    const minWidth = 70
    const styleAdjustedHeight = height - stylePixelOffset

    if (!query) {
      return <div className="generic-empty-state">Please add a query below</div>
    }

    if (!isLoading && !series.length) {
      return (
        <div className="generic-empty-state">Your query returned no data</div>
      )
    }

    return (
      <div>
        {series.map(({columns, values}, i) => {
          if (!values.legnth) {
            return <div key={i} className="generic-empty-state">No data</div>
          }
          return (
            <Table
              key={i}
              onColumnResizeEndCallback={this.handleColumnResize}
              isColumnResizing={false}
              rowHeight={rowHeight}
              rowsCount={values.length}
              width={containerWidth}
              ownerHeight={styleAdjustedHeight}
              height={styleAdjustedHeight}
              headerHeight={headerHeight}
            >
              {columns.map((columnName, colIndex) => (
                <Column
                  isResizable={true}
                  key={columnName}
                  columnKey={columnName}
                  header={<Cell>{columnName}</Cell>}
                  cell={({rowIndex}) => (
                    <CustomCell
                      columnName={columnName}
                      data={values[rowIndex][colIndex]}
                    />
                  )}
                  width={
                    columnWidths[columnName] || columns.length > 1
                      ? defaultColumnWidth
                      : containerWidth
                  }
                  minWidth={minWidth}
                />
              ))}
            </Table>
          )
        })}
      </div>
    )
  },
})

export default Dimensions({elementResize: true})(ChronoTable)
