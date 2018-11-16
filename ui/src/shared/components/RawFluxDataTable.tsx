// Libraries
import React, {PureComponent, MouseEvent, CSSProperties} from 'react'
import {Grid} from 'react-virtualized'
import memoizeOne from 'memoize-one'

// Components
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Utils
import {parseFiles} from 'src/shared/utils/rawFluxDataTable'

interface Props {
  files: string[]
  width: number
  height: number
}

interface State {
  scrollLeft: number
  scrollTop: number
}

const ROW_HEIGHT = 30
const MIN_COLUMN_WIDTH = 100
const PADDING = 10

class RawFluxDataTable extends PureComponent<Props, State> {
  public state = {scrollLeft: 0, scrollTop: 0}

  private parseFiles = memoizeOne(parseFiles)

  public render() {
    const {width, height, files} = this.props
    const {scrollTop, scrollLeft} = this.state
    const {data, maxColumnCount} = this.parseFiles(files)

    const tableWidth = width - PADDING * 2
    const tableHeight = height - PADDING * 2

    return (
      <div className="raw-flux-data-table" style={{padding: `${PADDING}px`}}>
        <FancyScrollbar
          style={{
            overflowY: 'hidden',
            width: tableWidth,
            height: tableHeight,
          }}
          autoHide={false}
          scrollTop={scrollTop}
          scrollLeft={scrollLeft}
          setScrollTop={this.onScrollbarsScroll}
        >
          {this.renderGrid(
            tableWidth,
            tableHeight,
            data,
            maxColumnCount,
            scrollLeft,
            scrollTop
          )}
        </FancyScrollbar>
      </div>
    )
  }

  private renderGrid(
    width: number,
    height: number,
    data: string[][],
    maxColumnCount: number,
    scrollLeft: number,
    scrollTop: number
  ): JSX.Element {
    const rowCount = data.length
    const columnWidth = Math.max(MIN_COLUMN_WIDTH, width / maxColumnCount)
    const style = this.gridStyle(columnWidth, maxColumnCount, rowCount)

    return (
      <Grid
        width={width}
        height={height}
        cellRenderer={this.renderCell(data)}
        columnCount={maxColumnCount}
        rowCount={rowCount}
        rowHeight={ROW_HEIGHT}
        columnWidth={columnWidth}
        scrollLeft={scrollLeft}
        scrollTop={scrollTop}
        style={style}
      />
    )
  }

  private gridStyle(
    columnWidth: number,
    maxColumnCount: number,
    rowCount: number
  ): CSSProperties {
    const width = columnWidth * maxColumnCount
    const height = ROW_HEIGHT * rowCount

    return {width, height}
  }

  private onScrollbarsScroll = (e: MouseEvent<HTMLElement>) => {
    e.preventDefault()
    e.stopPropagation()

    const {scrollTop, scrollLeft} = e.currentTarget

    this.setState({scrollLeft, scrollTop})
  }

  private renderCell = (data: string[][]) => ({
    columnIndex,
    key,
    rowIndex,
    style,
  }) => {
    const datum = data[rowIndex][columnIndex]

    return (
      <div
        key={key}
        style={style}
        className="raw-flux-data-table--cell"
        title={datum}
      >
        <div className="raw-flux-data-table--cell-bg">{datum}</div>
      </div>
    )
  }
}

export default RawFluxDataTable
