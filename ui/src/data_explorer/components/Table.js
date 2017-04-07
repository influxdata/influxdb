import React, {PropTypes} from 'react'
import {Table, Column, Cell} from 'fixed-data-table'
import Dimensions from 'react-dimensions'
import _ from 'lodash'
import moment from 'moment'

const {
  arrayOf,
  func,
  number,
  oneOfType,
  shape,
  string,
} = PropTypes

const emptyCells = {
  columns: [],
  values: [],
}

const defaultTableHeight = 1000

const CustomCell = React.createClass({
  propTypes: {
    data: oneOfType([number, string]).isRequired,
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
    onEditRawStatus: func,
    fetchTimeSeries: func.isRequired,
  },

  getInitialState() {
    return {
      cellData: emptyCells,
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
      const results = await this.props.fetchTimeSeries(query.host, undefined, query)
      this.setState({isLoading: false})

      if (!results) {
        return this.setState({cellData: emptyCells})
      }

      const cellData = _.get(results, ['series', '0'], {})
      this.setState({cellData})
    } catch (error) {
      this.setState({
        isLoading: false,
        cellData: emptyCells,
      })
      console.error(error)
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
    const {cellData, columnWidths, isLoading} = this.state
    const {columns, values} = cellData

    // adjust height to proper value by subtracting the heights of the UI around it
    // tab height, graph-container vertical padding, graph-heading height, multitable-header height
    const stylePixelOffset = 136

    const rowHeight = 34
    const defaultColumnWidth = 200
    const width = columns.length > 1 ? defaultColumnWidth : containerWidth
    const headerHeight = 30
    const minWidth = 70
    const styleAdjustedHeight = height - stylePixelOffset

    if (!query) {
      return <div className="generic-empty-state">Please add a query below</div>
    }

    if (!isLoading && !values.length) {
      return <div className="generic-empty-state">Your query returned no data</div>
    }

    return (
      <Table
        onColumnResizeEndCallback={this.handleColumnResize}
        isColumnResizing={false}
        rowHeight={rowHeight}
        rowsCount={values.length}
        width={containerWidth}
        ownerHeight={styleAdjustedHeight}
        height={styleAdjustedHeight}
        headerHeight={headerHeight}>
        {columns.map((columnName, colIndex) => {
          return (
            <Column
              isResizable={true}
              key={columnName}
              columnKey={columnName}
              header={<Cell>{columnName}</Cell>}
              cell={({rowIndex}) => {
                return <CustomCell columnName={columnName} data={values[rowIndex][colIndex]} />
              }}
              width={columnWidths[columnName] || width}
              minWidth={minWidth}
            />
          )
        })}
      </Table>
    )
  },
})

export default Dimensions({elementResize: true})(ChronoTable)
