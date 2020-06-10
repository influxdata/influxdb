// Libraries
import React, {PureComponent, CSSProperties} from 'react'
import _ from 'lodash'
import {Grid} from 'react-virtualized'

const ROW_HEIGHT = 27
const MIN_COLUMN_WIDTH = 150
const TIME_COLUMN_WIDTH = 300

interface Props {
  data: string[][]
  maxColumnCount: number
  width: number
  height: number
  scrollLeft: number
  scrollTop: number
}

interface State {
  headerRows: number[]
}

export default class extends PureComponent<Props, State> {
  public static getDerivedStateFromProps(nextProps: Props): Partial<State> {
    const headerRows = _.reduce(
      nextProps.data,
      (acc, row, index) => {
        if (row[0] === '#datatype') {
          acc.push(index)
        }

        return acc
      },
      []
    )

    return {headerRows}
  }
  constructor(props: Props) {
    super(props)

    this.state = {headerRows: []}
  }

  public render() {
    const {maxColumnCount, width, height, scrollTop, scrollLeft} = this.props

    return (
      <Grid
        width={width}
        height={height}
        cellRenderer={this.renderCell}
        columnCount={maxColumnCount}
        rowCount={this.rowCount}
        rowHeight={ROW_HEIGHT}
        columnWidth={this.columnWidth}
        scrollLeft={scrollLeft}
        scrollTop={scrollTop}
        style={this.gridStyle}
      />
    )
  }

  private get rowCount(): number {
    return this.props.data.length
  }

  private columnWidth = ({index}): number => {
    const {maxColumnCount, width} = this.props

    const isDateTimeColumn = _.find(this.state.headerRows, i => {
      return (this.getCellData(i, index) || '').includes('dateTime')
    })

    if (!isDateTimeColumn) {
      return Math.max(MIN_COLUMN_WIDTH, width / maxColumnCount)
    }

    return TIME_COLUMN_WIDTH
  }

  private get gridStyle(): CSSProperties {
    const width = this.calculateWidth()
    const height = ROW_HEIGHT * this.rowCount

    return {width, height}
  }

  private renderCell = ({columnIndex, key, rowIndex, style}) => {
    const datum = this.getCellData(rowIndex, columnIndex)

    return (
      <div
        key={key}
        style={style}
        className="raw-flux-data-table--cell"
        title={datum}
      >
        <div
          className="raw-flux-data-table--cell-bg"
          data-testid={`raw-flux-data-table--cell ${datum}`}
        >
          {datum}
        </div>
      </div>
    )
  }

  private calculateWidth(): number {
    const {maxColumnCount} = this.props
    return _.reduce(
      _.range(0, maxColumnCount),
      (acc, index) => acc + this.columnWidth({index}),
      0
    )
  }

  private getCellData(row, column) {
    const {data} = this.props
    return data[row][column]
  }
}
