import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import {DASHBOARD_LAYOUT_ROW_HEIGHT} from 'shared/constants'
import {timeSeriesToTable} from 'src/utils/timeSeriesToDygraph'
import {MultiGrid} from 'react-virtualized/dist/commonjs/MultiGrid'

class TableGraph extends Component {
  constructor(props) {
    super(props)
  }

  componentWillMount() {
    this._labels = []
    this._data = [[]]
  }

  componentWillUpdate(nextProps) {
    // TODO: determine if in dataExplorer
    const {labels, data} = timeSeriesToTable(nextProps.data)
    this._labels = labels
    this._data = data
  }

  cellRenderer = ({columnIndex, key, rowIndex, style}) => {
    const data = this._data
    return (
      <div key={key} style={style}>
        {data[rowIndex][columnIndex]}
      </div>
    )
  }

  render() {
    const data = this._data
    const columnCount = _.get(data, ['0', 'length'], 0)
    const rowCount = data.length
    const COLUMN_WIDTH = 300
    const ROW_HEIGHT = 50
    const tableWidth = this.gridContainer ? this.gridContainer.clientWidth : 0
    const tableHeight = this.gridContainer ? this.gridContainer.clientHeight : 0

    return (
      <div
        className="graph-container"
        ref={gridContainer => (this.gridContainer = gridContainer)}
      >
        {data.length > 1
          ? <MultiGrid
              fixedColumnCount={1}
              fixedRowCount={1}
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

const {arrayOf, number, shape} = PropTypes

TableGraph.propTypes = {
  cellHeight: number,
  data: arrayOf(shape()),
}

export default TableGraph
