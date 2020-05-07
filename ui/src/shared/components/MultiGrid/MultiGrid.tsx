import * as React from 'react'
import CellMeasurerCacheDecorator from './CellMeasurerCacheDecorator'
import {Grid} from 'react-virtualized'
import { DapperScrollbars } from '@influxdata/clockface'

const SCROLLBAR_SIZE_BUFFER = 20
type HeightWidthFunction = (arg: {index: number}) => number

export interface PropsMultiGrid {
  width: number
  height: number
  columnCount?: number
  classNameBottomLeftGrid?: string
  classNameBottomRightGrid?: string
  classNameTopLeftGrid?: string
  classNameTopRightGrid?: string
  enableFixedColumnScroll?: boolean
  enableFixedRowScroll?: boolean
  fixedColumnCount?: number
  fixedRowCount?: number
  style?: object
  styleBottomLeftGrid?: object
  styleBottomRightGrid?: object
  styleTopLeftGrid?: object
  styleTopRightGrid?: object
  scrollTop?: number
  scrollLeft?: number
  rowCount?: number
  rowHeight?: number | HeightWidthFunction
  columnWidth?: number | HeightWidthFunction
  onScroll?: (arg: object) => {}
  onSectionRendered?: () => {}
  cellRenderer?: (arg: object) => JSX.Element
  [key: string]: any // MultiGrid can accept any prop, and will rerender if they change
}

interface State {
  scrollLeft: number
  scrollTop: number
  scrollbarSize: number
  showHorizontalScrollbar: boolean
  showVerticalScrollbar: boolean
}

/**
 * Renders 1, 2, or 4 Grids depending on configuration.
 * A main (body) Grid will always be rendered.
 * Optionally, 1-2 Grids for sticky header rows will also be rendered.
 * If no sticky columns, only 1 sticky header Grid will be rendered.
 * If sticky columns, 2 sticky header Grids will be rendered.
 */
class MultiGrid extends React.PureComponent<PropsMultiGrid, State> {
  public static defaultProps = {
    classNameBottomLeftGrid: '',
    classNameBottomRightGrid: '',
    classNameTopLeftGrid: '',
    classNameTopRightGrid: '',
    enableFixedColumnScroll: false,
    enableFixedRowScroll: false,
    fixedColumnCount: 0,
    fixedRowCount: 0,
    scrollToColumn: -1,
    scrollToRow: -1,
    style: {},
    styleBottomLeftGrid: {},
    styleBottomRightGrid: {},
    styleTopLeftGrid: {},
    styleTopRightGrid: {},
  }

  public static getDerivedStateFromProps(
    nextProps: PropsMultiGrid,
    prevState: State
  ) {
    if (
      nextProps.scrollLeft !== prevState.scrollLeft ||
      nextProps.scrollTop !== prevState.scrollTop
    ) {
      return {
        scrollLeft:
          nextProps.scrollLeft != null && nextProps.scrollLeft >= 0
            ? nextProps.scrollLeft
            : prevState.scrollLeft,
        scrollTop:
          nextProps.scrollTop != null && nextProps.scrollTop >= 0
            ? nextProps.scrollTop
            : prevState.scrollTop,
      }
    }

    return null
  }

  private deferredInvalidateColumnIndex: number = 0
  private deferredInvalidateRowIndex: number = 0
  private bottomLeftGrid: Grid
  private bottomRightGrid: Grid
  private topLeftGrid: Grid
  private topRightGrid: Grid
  private deferredMeasurementCacheBottomLeftGrid: CellMeasurerCacheDecorator
  private deferredMeasurementCacheBottomRightGrid: CellMeasurerCacheDecorator
  private deferredMeasurementCacheTopRightGrid: CellMeasurerCacheDecorator
  private leftGridWidth: number | null = 0
  private topGridHeight: number | null = 0
  private lastRenderedColumnWidth: number | HeightWidthFunction
  private lastRenderedFixedColumnCount: number = 0
  private lastRenderedFixedRowCount: number = 0
  private lastRenderedRowHeight: number | HeightWidthFunction
  private bottomRightGridStyle: object | null
  private topRightGridStyle: object | null
  private lastRenderedStyle: object | null
  private lastRenderedHeight: number = 0
  private lastRenderedWidth: number = 0
  private containerTopStyle: object | null
  private containerBottomStyle: object | null
  private containerOuterStyle: object | null
  private lastRenderedStyleBottomLeftGrid: object | null
  private lastRenderedStyleBottomRightGrid: object | null
  private lastRenderedStyleTopLeftGrid: object | null
  private lastRenderedStyleTopRightGrid: object | null
  private bottomLeftGridStyle: object | null
  private topLeftGridStyle: object | null

  constructor(props: PropsMultiGrid, context) {
    super(props, context)

    this.state = {
      scrollLeft: 0,
      scrollTop: 0,
      scrollbarSize: 0,
      showHorizontalScrollbar: false,
      showVerticalScrollbar: false,
    }

    const {deferredMeasurementCache, fixedColumnCount, fixedRowCount} = props

    this.maybeCalculateCachedStyles(true)

    if (deferredMeasurementCache) {
      this.deferredMeasurementCacheBottomLeftGrid =
        fixedRowCount > 0
          ? new CellMeasurerCacheDecorator({
              cellMeasurerCache: deferredMeasurementCache,
              columnIndexOffset: 0,
              rowIndexOffset: fixedRowCount,
            })
          : deferredMeasurementCache

      this.deferredMeasurementCacheBottomRightGrid =
        fixedColumnCount > 0 || fixedRowCount > 0
          ? new CellMeasurerCacheDecorator({
              cellMeasurerCache: deferredMeasurementCache,
              columnIndexOffset: fixedColumnCount,
              rowIndexOffset: fixedRowCount,
            })
          : deferredMeasurementCache

      this.deferredMeasurementCacheTopRightGrid =
        fixedColumnCount > 0
          ? new CellMeasurerCacheDecorator({
              cellMeasurerCache: deferredMeasurementCache,
              columnIndexOffset: fixedColumnCount,
              rowIndexOffset: 0,
            })
          : deferredMeasurementCache
    }
  }

  public forceUpdateGrids() {
    if (this.bottomLeftGrid) {
      this.bottomLeftGrid.forceUpdate()
    }
    if (this.bottomRightGrid) {
      this.bottomRightGrid.forceUpdate()
    }
    if (this.topLeftGrid) {
      this.topLeftGrid.forceUpdate()
    }
    if (this.topRightGrid) {
      this.topRightGrid.forceUpdate()
    }
  }

  /** See Grid#invalidateCellSizeAfterRender */
  public invalidateCellSizeAfterRender({columnIndex = 0, rowIndex = 0} = {}) {
    this.deferredInvalidateColumnIndex =
      typeof this.deferredInvalidateColumnIndex === 'number'
        ? Math.min(this.deferredInvalidateColumnIndex, columnIndex)
        : columnIndex
    this.deferredInvalidateRowIndex =
      typeof this.deferredInvalidateRowIndex === 'number'
        ? Math.min(this.deferredInvalidateRowIndex, rowIndex)
        : rowIndex
  }

  /** See Grid#measureAllCells */
  public measureAllCells() {
    if (this.bottomLeftGrid) {
      this.bottomLeftGrid.measureAllCells()
    }
    if (this.bottomRightGrid) {
      this.bottomRightGrid.measureAllCells()
    }
    if (this.topLeftGrid) {
      this.topLeftGrid.measureAllCells()
    }
    if (this.topRightGrid) {
      this.topRightGrid.measureAllCells()
    }
  }

  public recomputeGridSize({columnIndex = 0, rowIndex = 0} = {}) {
    const {fixedColumnCount, fixedRowCount} = this.props

    const adjustedColumnIndex = Math.max(0, columnIndex - fixedColumnCount)
    const adjustedRowIndex = Math.max(0, rowIndex - fixedRowCount)

    if (this.bottomLeftGrid) {
      this.bottomLeftGrid.recomputeGridSize({
        columnIndex,
        rowIndex: adjustedRowIndex,
      })
    }
    if (this.bottomRightGrid) {
      this.bottomRightGrid.recomputeGridSize({
        columnIndex: adjustedColumnIndex,
        rowIndex: adjustedRowIndex,
      })
    }

    if (this.topLeftGrid) {
      this.topLeftGrid.recomputeGridSize({
        columnIndex,
        rowIndex,
      })
    }

    if (this.topRightGrid) {
      this.topRightGrid.recomputeGridSize({
        columnIndex: adjustedColumnIndex,
        rowIndex,
      })
    }

    this.leftGridWidth = null
    this.topGridHeight = null
    this.maybeCalculateCachedStyles(true)
  }

  public componentDidMount() {
    const {scrollLeft, scrollTop} = this.props

    if (scrollLeft > 0 || scrollTop > 0) {
      const newState: Partial<State> = {}

      if (scrollLeft > 0) {
        newState.scrollLeft = scrollLeft
      }

      if (scrollTop > 0) {
        newState.scrollTop = scrollTop
      }

      this.setState({...this.state, ...newState})
    }
    this.handleInvalidatedGridSize()
  }

  public componentDidUpdate() {
    this.handleInvalidatedGridSize()
  }

  public render() {
    const {
      onScroll,
      onSectionRendered,
      scrollToRow,
      scrollToColumn,
      ...rest
    } = this.props

    this.prepareForRender()

    // Don't render any of our Grids if there are no cells.
    // This mirrors what Grid does,
    // And prevents us from recording inaccurage measurements when used with CellMeasurer.
    if (this.props.width === 0 || this.props.height === 0) {
      return null
    }

    // scrollTop and scrollLeft props are explicitly filtered out and ignored

    const {scrollLeft, scrollTop} = this.state

    return (
      <div style={this.containerOuterStyle}>
        <div style={this.containerTopStyle}>
          {this.renderTopLeftGrid(rest)}
          {this.renderTopRightGrid({
            ...rest,
            ...onScroll,
            scrollLeft,
          })}
        </div>
        <div style={this.containerBottomStyle}>
          {this.renderBottomLeftGrid({
            ...rest,
            onScroll,
            scrollTop,
          })}
          {this.renderBottomRightGrid({
            ...rest,
            onScroll,
            onSectionRendered,
            scrollLeft,
            scrollToColumn,
            scrollToRow,
            scrollTop,
          })}
        </div>
      </div>
    )
  }

  public cellRendererBottomLeftGrid = ({rowIndex, ...rest}) => {
    const {cellRenderer, fixedRowCount, rowCount} = this.props

    if (rowIndex === rowCount - fixedRowCount) {
      return (
        <div
          key={rest.key}
          style={{
            ...rest.style,
            height: SCROLLBAR_SIZE_BUFFER,
          }}
        />
      )
    } else {
      return cellRenderer({
        ...rest,
        parent: this,
        rowIndex: rowIndex + fixedRowCount,
      })
    }
  }

  private getBottomGridHeight(props: PropsMultiGrid) {
    const {height} = props

    const topGridHeight = this.getTopGridHeight(props)

    return height - topGridHeight
  }

  private getLeftGridWidth(props: PropsMultiGrid) {
    const {fixedColumnCount, columnWidth} = props

    if (this.leftGridWidth == null) {
      if (typeof columnWidth === 'function') {
        let leftGridWidth = 0

        for (let index = 0; index < fixedColumnCount; index++) {
          leftGridWidth += columnWidth({index})
        }

        this.leftGridWidth = leftGridWidth
      } else {
        this.leftGridWidth = columnWidth * fixedColumnCount
      }
    }

    return this.leftGridWidth
  }

  private getRightGridWidth(props: PropsMultiGrid) {
    const {width} = props

    const leftGridWidth = this.getLeftGridWidth(props)
    const result = width - leftGridWidth

    return result
  }

  private getTopGridHeight(props: PropsMultiGrid) {
    const {fixedRowCount, rowHeight} = props

    if (this.topGridHeight == null) {
      if (typeof rowHeight === 'function') {
        let topGridHeight = 0

        for (let index = 0; index < fixedRowCount; index++) {
          topGridHeight += rowHeight({index})
        }

        this.topGridHeight = topGridHeight
      } else {
        this.topGridHeight = rowHeight * fixedRowCount
      }
    }

    return this.topGridHeight
  }

  private onScrollbarsScroll = (e: React.MouseEvent<HTMLElement>) => {
    const {target} = e
    this.onScroll(target)
  }

  private onScroll = scrollInfo => {
    const {scrollLeft, scrollTop} = scrollInfo
    this.setState({
      scrollLeft,
      scrollTop,
    })

    const {onScroll} = this.props
    if (onScroll) {
      onScroll(scrollInfo)
    }
  }

  private onScrollLeft = scrollInfo => {
    const {scrollLeft} = scrollInfo
    this.onScroll({
      scrollLeft,
      scrollTop: this.state.scrollTop,
    })
  }

  private renderBottomLeftGrid(props) {
    const {fixedColumnCount, fixedRowCount, rowCount} = props

    if (!fixedColumnCount) {
      return null
    }

    const width = this.getLeftGridWidth(props)
    const height = this.getBottomGridHeight(props)

    return (
      <Grid
        {...props}
        cellRenderer={this.cellRendererBottomLeftGrid}
        className={this.props.classNameBottomLeftGrid}
        columnCount={fixedColumnCount}
        deferredMeasurementCache={this.deferredMeasurementCacheBottomLeftGrid}
        onScroll={this.onScroll}
        height={height}
        ref={this.bottomLeftGridRef}
        rowCount={Math.max(0, rowCount - fixedRowCount)}
        rowHeight={this.rowHeightBottomGrid}
        style={{
          ...this.bottomLeftGridStyle,
        }}
        tabIndex={null}
        width={width}
      />
    )
  }

  private renderBottomRightGrid(props) {
    const {
      columnCount,
      fixedColumnCount,
      fixedRowCount,
      rowCount,
      scrollToColumn,
      scrollToRow,
    } = props

    const width = this.getRightGridWidth(props)
    const height = this.getBottomGridHeight(props)

    return (
      <DapperScrollbars
        style={{...this.bottomRightGridStyle, width, height}}
        autoHide={true}
        scrollTop={this.state.scrollTop}
        scrollLeft={this.state.scrollLeft}
        onScroll={this.onScrollbarsScroll}
      >
        <Grid
          {...props}
          cellRenderer={this.cellRendererBottomRightGrid}
          className={this.props.classNameBottomRightGrid}
          columnCount={Math.max(0, columnCount - fixedColumnCount)}
          columnWidth={this.columnWidthRightGrid}
          deferredMeasurementCache={
            this.deferredMeasurementCacheBottomRightGrid
          }
          height={height}
          ref={this.bottomRightGridRef}
          rowCount={Math.max(0, rowCount - fixedRowCount)}
          rowHeight={this.rowHeightBottomGrid}
          onScroll={this.onScroll}
          scrollToColumn={scrollToColumn - fixedColumnCount}
          scrollToRow={scrollToRow - fixedRowCount}
          style={{
            ...this.bottomRightGridStyle,
            overflowX: false,
            overflowY: true,
            left: 0,
          }}
          width={width}
        />
      </DapperScrollbars>
    )
  }

  private renderTopLeftGrid(props) {
    const {fixedColumnCount, fixedRowCount} = props

    if (!fixedColumnCount || !fixedRowCount) {
      return null
    }

    return (
      <Grid
        {...props}
        className={this.props.classNameTopLeftGrid}
        columnCount={fixedColumnCount}
        height={this.getTopGridHeight(props)}
        ref={this.topLeftGridRef}
        rowCount={fixedRowCount}
        style={this.topLeftGridStyle}
        tabIndex={null}
        width={this.getLeftGridWidth(props)}
      />
    )
  }

  private renderTopRightGrid(props) {
    const {
      columnCount,
      enableFixedRowScroll,
      fixedColumnCount,
      fixedRowCount,
      scrollLeft,
    } = props

    if (!fixedRowCount) {
      return null
    }

    const width = this.getRightGridWidth(props)
    const height = this.getTopGridHeight(props)

    return (
      <Grid
        {...props}
        cellRenderer={this.cellRendererTopRightGrid}
        className={this.props.classNameTopRightGrid}
        columnCount={Math.max(0, columnCount - fixedColumnCount)}
        columnWidth={this.columnWidthRightGrid}
        deferredMeasurementCache={this.deferredMeasurementCacheTopRightGrid}
        height={height}
        onScroll={enableFixedRowScroll ? this.onScrollLeft : undefined}
        ref={this.topRightGridRef}
        rowCount={fixedRowCount}
        scrollLeft={scrollLeft}
        style={this.topRightGridStyle}
        tabIndex={null}
        width={width}
      />
    )
  }

  private rowHeightBottomGrid = ({index}) => {
    const {fixedRowCount, rowCount, rowHeight} = this.props
    const {scrollbarSize, showVerticalScrollbar} = this.state

    // An extra cell is added to the count
    // This gives the smaller Grid extra room for offset,
    // In case the main (bottom right) Grid has a scrollbar
    // If no scrollbar, the extra space is overflow:hidden anyway
    if (showVerticalScrollbar && index === rowCount - fixedRowCount) {
      return scrollbarSize
    }

    return typeof rowHeight === 'function'
      ? rowHeight({index: index + fixedRowCount})
      : rowHeight
  }

  private topLeftGridRef = ref => {
    this.topLeftGrid = ref
  }

  private topRightGridRef = ref => {
    this.topRightGrid = ref
  }

  /**
   * Avoid recreating inline styles each render; this bypasses Grid's shallowCompare.
   * This method recalculates styles only when specific props change.
   */
  private maybeCalculateCachedStyles(resetAll) {
    const {
      columnWidth,
      height,
      fixedColumnCount,
      fixedRowCount,
      rowHeight,
      style,
      styleBottomLeftGrid,
      styleBottomRightGrid,
      styleTopLeftGrid,
      styleTopRightGrid,
      width,
    } = this.props

    const sizeChange =
      resetAll ||
      height !== this.lastRenderedHeight ||
      width !== this.lastRenderedWidth
    const leftSizeChange =
      resetAll ||
      columnWidth !== this.lastRenderedColumnWidth ||
      fixedColumnCount !== this.lastRenderedFixedColumnCount
    const topSizeChange =
      resetAll ||
      fixedRowCount !== this.lastRenderedFixedRowCount ||
      rowHeight !== this.lastRenderedRowHeight

    if (resetAll || sizeChange || style !== this.lastRenderedStyle) {
      this.containerOuterStyle = {
        height,
        overflow: 'visible', // Let :focus outline show through
        width,
        ...style,
      }
    }

    if (resetAll || sizeChange || topSizeChange) {
      this.containerTopStyle = {
        height: this.getTopGridHeight(this.props),
        position: 'relative',
        width,
      }

      this.containerBottomStyle = {
        height: height - this.getTopGridHeight(this.props),
        overflow: 'visible', // Let :focus outline show through
        position: 'relative',
        width,
      }
    }

    if (
      resetAll ||
      styleBottomLeftGrid !== this.lastRenderedStyleBottomLeftGrid
    ) {
      this.bottomLeftGridStyle = {
        left: 0,
        overflowY: 'hidden',
        overflowX: 'hidden',
        position: 'absolute',
        ...styleBottomLeftGrid,
      }
    }

    if (
      resetAll ||
      leftSizeChange ||
      styleBottomRightGrid !== this.lastRenderedStyleBottomRightGrid
    ) {
      this.bottomRightGridStyle = {
        left: this.getLeftGridWidth(this.props),
        position: 'absolute',
        ...styleBottomRightGrid,
      }
    }

    if (resetAll || styleTopLeftGrid !== this.lastRenderedStyleTopLeftGrid) {
      this.topLeftGridStyle = {
        left: 0,
        overflowX: 'hidden',
        overflowY: 'hidden',
        position: 'absolute',
        top: 0,
        ...styleTopLeftGrid,
      }
    }

    if (
      resetAll ||
      leftSizeChange ||
      styleTopRightGrid !== this.lastRenderedStyleTopRightGrid
    ) {
      this.topRightGridStyle = {
        left: this.getLeftGridWidth(this.props),
        overflowX: 'hidden',
        overflowY: 'hidden',
        position: 'absolute',
        top: 0,
        ...styleTopRightGrid,
      }
    }

    this.lastRenderedColumnWidth = columnWidth
    this.lastRenderedFixedColumnCount = fixedColumnCount
    this.lastRenderedFixedRowCount = fixedRowCount
    this.lastRenderedHeight = height
    this.lastRenderedRowHeight = rowHeight
    this.lastRenderedStyle = style
    this.lastRenderedStyleBottomLeftGrid = styleBottomLeftGrid
    this.lastRenderedStyleBottomRightGrid = styleBottomRightGrid
    this.lastRenderedStyleTopLeftGrid = styleTopLeftGrid
    this.lastRenderedStyleTopRightGrid = styleTopRightGrid
    this.lastRenderedWidth = width
  }

  private bottomLeftGridRef = ref => {
    this.bottomLeftGrid = ref
  }

  private bottomRightGridRef = ref => {
    this.bottomRightGrid = ref
  }

  private cellRendererBottomRightGrid = ({columnIndex, rowIndex, ...rest}) => {
    const {cellRenderer, fixedColumnCount, fixedRowCount} = this.props

    return cellRenderer({
      ...rest,
      columnIndex: columnIndex + fixedColumnCount,
      parent: this,
      rowIndex: rowIndex + fixedRowCount,
    })
  }

  private cellRendererTopRightGrid = ({columnIndex, ...rest}) => {
    const {cellRenderer, columnCount, fixedColumnCount} = this.props

    if (columnIndex === columnCount - fixedColumnCount) {
      return (
        <div
          key={rest.key}
          style={{
            ...rest.style,
            width: SCROLLBAR_SIZE_BUFFER,
          }}
        />
      )
    } else {
      return cellRenderer({
        ...rest,
        columnIndex: columnIndex + fixedColumnCount,
        parent: this,
      })
    }
  }

  private columnWidthRightGrid = ({index}) => {
    const {columnCount, fixedColumnCount, columnWidth} = this.props
    const {scrollbarSize, showHorizontalScrollbar} = this.state

    // An extra cell is added to the count
    // This gives the smaller Grid extra room for offset,
    // In case the main (bottom right) Grid has a scrollbar
    // If no scrollbar, the extra space is overflow:hidden anyway
    if (showHorizontalScrollbar && index === columnCount - fixedColumnCount) {
      return scrollbarSize
    }

    return typeof columnWidth === 'function'
      ? columnWidth({index: index + fixedColumnCount})
      : columnWidth
  }

  private handleInvalidatedGridSize() {
    if (typeof this.deferredInvalidateColumnIndex === 'number') {
      const columnIndex = this.deferredInvalidateColumnIndex
      const rowIndex = this.deferredInvalidateRowIndex

      this.deferredInvalidateColumnIndex = null
      this.deferredInvalidateRowIndex = null

      this.recomputeGridSize({
        columnIndex,
        rowIndex,
      })
      this.forceUpdate()
    }
  }

  private prepareForRender() {
    if (
      this.lastRenderedColumnWidth !== this.props.columnWidth ||
      this.lastRenderedFixedColumnCount !== this.props.fixedColumnCount
    ) {
      this.leftGridWidth = null
    }

    if (
      this.lastRenderedFixedRowCount !== this.props.fixedRowCount ||
      this.lastRenderedRowHeight !== this.props.rowHeight
    ) {
      this.topGridHeight = null
    }

    this.maybeCalculateCachedStyles(false)

    this.lastRenderedColumnWidth = this.props.columnWidth
    this.lastRenderedFixedColumnCount = this.props.fixedColumnCount
    this.lastRenderedFixedRowCount = this.props.fixedRowCount
    this.lastRenderedRowHeight = this.props.rowHeight
  }
}

export default MultiGrid
