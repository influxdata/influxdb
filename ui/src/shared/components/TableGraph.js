import React, {PropTypes, Component} from 'react'
import _ from 'lodash'
import {timeSeriesToTable} from 'src/utils/timeSeriesToDygraph'
import {MultiGrid} from 'react-virtualized'

class TableGraph extends Component {
  constructor(props) {
    super(props)
    this.state = {
      hoveredColumnIndex: -1,
      hoveredRowIndex: -1,
    }
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

  handleHover = (columnIndex, rowIndex) => () => {
    this.setState({hoveredColumnIndex: columnIndex, hoveredRowIndex: rowIndex})
  }

  handleMouseOut = () => {
    this.setState({hoveredColumnIndex: -1, hoveredRowIndex: -1})
  }

  cellRenderer = ({columnIndex, key, rowIndex, style}) => {
    const data = this._data
    const className =
      columnIndex === this.state.hoveredColumnIndex ||
      rowIndex === this.state.hoveredRowIndex
        ? 'tablecell hovered'
        : 'tablecell'

    return (
      <div
        key={key}
        style={style}
        className={className}
        onMouseOver={this.handleHover(columnIndex, rowIndex)}
      >
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
        onMouseOut={this.handleMouseOut}
      >
        {data.length > 1 &&
          <MultiGrid
            fixedColumnCount={1}
            fixedRowCount={1}
            cellRenderer={this.cellRenderer}
            columnCount={columnCount}
            columnWidth={COLUMN_WIDTH}
            height={tableHeight}
            rowCount={rowCount}
            rowHeight={ROW_HEIGHT}
            width={tableWidth - 32}
            hoveredColumnIndex={this.state.hoveredColumnIndex}
            hoveredRowIndex={this.state.hoveredRowIndex}
          />}
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
