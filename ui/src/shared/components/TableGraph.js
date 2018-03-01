import React, {PropTypes, Component} from 'react'
import {Grid} from 'react-virtualized'
import _ from 'lodash'
// import uuid from 'node-uuid'

import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'shared/constants'
import timeSeriesToDygraph from 'src/utils/timeSeriesToDygraph'

class TableGraph extends Component {
  constructor(props) {
    super(props)
  }

  componentWillUpdate(nextProps) {
    // TODO: determine if in dataExplorer
    this._timeSeries = timeSeriesToDygraph(nextProps.data, false)
  }

  cellRenderer = ({columnIndex, key, rowIndex, style}) => {
    const data = _.get(this._timeSeries, 'timeSeries', [[]])
    console.log(data[0][0].toString())
    return (
      <div key={key} style={style}>
        {data[columnIndex][rowIndex]
          ? data[columnIndex][rowIndex].toString()
          : ''}
      </div>
    )
  }
  render() {
    const data = _.get(this._timeSeries, 'timeSeries', [[]])
    const columns = _.get(this._timeSeries, 'labels', [])

    const {cellHeight} = this.props
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_WIDTH = 50
    const ROW_HEIGHT = 50
    const tableWidth = this.gridContainer ? this.gridContainer.clientWidth : 400
    const tableHeight = cellHeight * DASHBOARD_LAYOUT_ROW_HEIGHT - 64

    return (
      <div
        className="graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
      >
        {data.length > 1
          ? <Grid
              cellRenderer={this.cellRenderer}
              columnCount={columnCount}
              columnWidth={COLUMN_WIDTH}
              height={tableHeight}
              rowCount={rowCount}
              rowHeight={ROW_HEIGHT}
              width={tableWidth - 32}
            />
          : null}
      </div>
    )
  }
}

export default TableGraph
