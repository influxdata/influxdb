import _ from 'lodash'
import moment from 'moment'
import classnames from 'classnames'
import React, {Component, MouseEvent, CSSProperties} from 'react'
import {Grid, AutoSizer, InfiniteLoader} from 'react-virtualized'
import {color} from 'd3-color'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import ExpandableMessage from 'src/logs/components/expandable_message/ExpandableMessage'
import {getDeep} from 'src/utils/wrappers'

import {colorForSeverity} from 'src/logs/utils/colors'
import {
  ROW_HEIGHT,
  calculateRowCharWidth,
  getColumnFromData,
  getValueFromData,
  getValuesFromData,
  isClickable,
  formatColumnValue,
  header,
  getColumnWidth,
  getMessageWidth,
  getColumnsFromData,
} from 'src/logs/utils/table'

import {
  SeverityFormatOptions,
  SeverityColorOptions,
  SeverityLevelOptions,
} from 'src/logs/constants'

import {TimeRange} from 'src/types'
import {
  TableData,
  LogsTableColumn,
  SeverityFormat,
  SeverityLevelColor,
} from 'src/types/logs'
import {INITIAL_LIMIT} from 'src/logs/actions'

interface Props {
  data: TableData
  isScrolledToTop: boolean
  onScrollVertical: () => void
  onScrolledToTop: () => void
  onTagSelection: (selection: {tag: string; key: string}) => void
  fetchMore: (time: string) => Promise<void>
  fetchNewer: (time: string) => void
  hasScrolled: boolean
  count: number
  timeRange: TimeRange
  queryCount: number
  tableColumns: LogsTableColumn[]
  severityFormat: SeverityFormat
  severityLevelColors: SeverityLevelColor[]
  scrollToRow?: number
  tableInfiniteData: {
    forward: TableData
    backward: TableData
  }
  onChooseCustomTime: (time: string) => void
}

interface State {
  scrollLeft: number
  scrollTop: number
  currentRow: number
  currentMessageWidth: number
  isMessageVisible: boolean
  lastQueryTime: number
  firstQueryTime: number
  visibleColumnsCount: number
}

const calculateScrollTop = scrollToRow => {
  return scrollToRow * ROW_HEIGHT
}

class LogsTable extends Component<Props, State> {
  public static getDerivedStateFromProps(props, state): State {
    const {
      isScrolledToTop,
      scrollToRow,
      data,
      tableColumns,
      severityFormat,
    } = props
    const currentMessageWidth = getMessageWidth(
      data,
      tableColumns,
      severityFormat
    )

    let lastQueryTime = _.get(state, 'lastQueryTime', null)
    let firstQueryTime = _.get(state, 'firstQueryTime', null)
    let scrollTop = _.get(state, 'scrollTop', 0)
    if (isScrolledToTop) {
      lastQueryTime = null
      firstQueryTime = null
      scrollTop = 0
    }

    if (_.isNumber(scrollToRow)) {
      scrollTop = calculateScrollTop(scrollToRow)
    }

    const scrollLeft = _.get(state, 'scrollLeft', 0)

    let isMessageVisible: boolean = false
    const visibleColumnsCount = props.tableColumns.filter(c => {
      if (c.internalName === 'message') {
        isMessageVisible = c.visible
      }
      return c.visible
    }).length

    return {
      ...state,
      isQuerying: false,
      lastQueryTime,
      firstQueryTime,
      scrollTop,
      scrollLeft,
      currentRow: -1,
      currentMessageWidth,
      isMessageVisible,
      visibleColumnsCount,
      scrollToRow,
    }
  }

  private grid: Grid | null
  private headerGrid: React.RefObject<Grid>

  constructor(props: Props) {
    super(props)

    this.grid = null
    this.headerGrid = React.createRef()

    let isMessageVisible: boolean = false
    const visibleColumnsCount = props.tableColumns.filter(c => {
      if (c.internalName === 'message') {
        isMessageVisible = c.visible
      }
      return c.visible
    }).length

    this.state = {
      scrollTop: 0,
      scrollLeft: 0,
      currentRow: -1,
      currentMessageWidth: 0,
      lastQueryTime: null,
      firstQueryTime: null,
      isMessageVisible,
      visibleColumnsCount,
    }
  }

  public componentDidUpdate() {
    if (this.isTableEmpty) {
      return
    }

    if (this.grid) {
      this.grid.recomputeGridSize()
    }

    if (this.headerGrid.current) {
      this.headerGrid.current.recomputeGridSize()
    }
  }

  public componentDidMount() {
    window.addEventListener('resize', this.handleWindowResize)
    if (this.grid) {
      this.grid.recomputeGridSize()
    }

    if (this.headerGrid.current) {
      this.headerGrid.current.recomputeGridSize()
    }
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.handleWindowResize)
  }

  public render() {
    const columnCount = Math.max(getColumnsFromData(this.props.data).length, 0)

    if (this.isTableEmpty) {
      return this.emptyTable
    }

    return (
      <div
        className="logs-viewer--table-container"
        onMouseOut={this.handleMouseOut}
      >
        <AutoSizer>
          {({width}) => (
            <Grid
              ref={this.headerGrid}
              height={ROW_HEIGHT}
              rowHeight={ROW_HEIGHT}
              rowCount={1}
              width={width}
              scrollLeft={this.state.scrollLeft}
              onScroll={this.handleHeaderScroll}
              cellRenderer={this.headerRenderer}
              columnCount={columnCount}
              columnWidth={this.getColumnWidth}
            />
          )}
        </AutoSizer>
        <InfiniteLoader
          isRowLoaded={this.isRowLoaded}
          loadMoreRows={this.loadMoreBelowRows}
          rowCount={this.rowCount() + INITIAL_LIMIT}
        >
          {({registerChild, onRowsRendered}) => (
            <AutoSizer>
              {({width, height}) => (
                <FancyScrollbar
                  style={{
                    width,
                    height,
                    marginTop: `${ROW_HEIGHT}px`,
                  }}
                  setScrollTop={this.handleScrollbarScroll}
                  scrollTop={this.state.scrollTop}
                  autoHide={false}
                >
                  <Grid
                    rowHeight={ROW_HEIGHT}
                    {...this.gridProperties(
                      width,
                      height,
                      onRowsRendered,
                      columnCount,
                      registerChild
                    )}
                    style={{
                      height: this.calculateTotalHeight(),
                      overflowY: 'hidden',
                    }}
                  />
                </FancyScrollbar>
              )}
            </AutoSizer>
          )}
        </InfiniteLoader>
      </div>
    )
  }

  private gridProperties = (
    width: number,
    height: number,
    onRowsRendered: (params: {startIndex: number; stopIndex: number}) => void,
    columnCount: number,
    registerChild: (g: Grid) => void
  ) => {
    const {scrollToRow} = this.props
    const {scrollLeft, scrollTop} = this.state
    const result: any = {
      width,
      height,
      rowCount: this.rowCount(),
      scrollLeft,
      scrollTop,
      cellRenderer: this.cellRenderer,
      onSectionRendered: this.handleRowRender(onRowsRendered),
      onScroll: this.handleGridScroll,
      columnCount,
      columnWidth: this.getColumnWidth,
      ref: (ref: Grid) => {
        registerChild(ref)
        this.grid = ref
      },
    }

    if (_.isNumber(scrollToRow)) {
      result.scrollToRow = scrollToRow
    }

    return result
  }

  private handleGridScroll = ({scrollLeft}) => {
    this.handleScroll({scrollLeft})
  }

  private handleScrollbarScroll = (e: MouseEvent<JSX.Element>): void => {
    e.stopPropagation()
    e.preventDefault()
    const {scrollTop} = e.target as HTMLElement

    this.handleScroll({
      scrollTop,
    })
  }

  private handleScroll = scrollInfo => {
    if (_.has(scrollInfo, 'scrollTop')) {
      const {scrollTop} = scrollInfo
      const previousTop = this.state.scrollTop

      this.setState({scrollTop})

      if (scrollTop < 200 && scrollTop <= previousTop) {
        this.loadMoreAboveRows()
      }

      if (scrollTop === 0) {
        this.props.onScrolledToTop()
      } else if (scrollTop !== previousTop) {
        this.props.onScrollVertical()
      }
    }

    if (_.has(scrollInfo, 'scrollLeft')) {
      const {scrollLeft} = scrollInfo

      this.setState({scrollLeft})
    }
  }

  private handleRowRender = onRowsRendered => {
    return ({rowStartIndex, rowStopIndex}) => {
      onRowsRendered({startIndex: rowStartIndex, stopIndex: rowStopIndex})
    }
  }

  private loadMoreAboveRows = async () => {
    // Prevent multiple queries at the same time
    const {queryCount} = this.props
    if (queryCount > 0) {
      return
    }

    const data = getValuesFromData(this.props.tableInfiniteData.forward)
    const backwardData = getValuesFromData(
      this.props.tableInfiniteData.backward
    )
    const firstTime = getDeep(
      data,
      '0.0',
      getDeep(backwardData, '0.0', new Date().getTime())
    )

    const {firstQueryTime} = this.state
    if (firstQueryTime && firstQueryTime > firstTime) {
      return
    }

    this.setState({firstQueryTime: firstTime})
    await this.props.fetchNewer(moment(firstTime).toISOString())
  }

  private loadMoreBelowRows = async () => {
    // Prevent multiple queries at the same time
    const {queryCount} = this.props
    if (queryCount > 0) {
      return
    }

    const data = getValuesFromData(this.props.tableInfiniteData.backward)
    const forwardData = getValuesFromData(this.props.tableInfiniteData.forward)

    const lastTime = getDeep(
      data,
      `${data.length - 1}.0`,
      getDeep(forwardData, `${forwardData.length - 1}.0`, new Date().getTime())
    )

    // Guard against fetching on scrolling back up then down
    const {lastQueryTime} = this.state
    if (lastQueryTime && lastQueryTime <= lastTime) {
      return
    }

    this.setState({lastQueryTime: lastTime})
    await this.props.fetchMore(moment(lastTime).toISOString())
  }

  private rowCount = (): number => {
    const data = this.props.tableInfiniteData
    return (
      getDeep<number>(data, 'forward.values.length', 0) +
      getDeep<number>(data, 'backward.values.length', 0)
    )
  }

  private isRowLoaded = ({index}) => {
    return index < this.rowCount() - 1
  }

  private handleWindowResize = () => {
    this.setState({
      currentMessageWidth: getMessageWidth(
        this.props.data,
        this.props.tableColumns,
        this.props.severityFormat
      ),
    })
  }

  private handleHeaderScroll = ({scrollLeft}): void =>
    this.setState({scrollLeft})

  private getColumnWidth = ({index}: {index: number}): number => {
    const {severityFormat} = this.props
    const column = getColumnFromData(this.props.data, index)
    const {
      currentMessageWidth,
      isMessageVisible,
      visibleColumnsCount,
    } = this.state

    switch (column) {
      case 'message':
        return currentMessageWidth
      default:
        let columnKey = column
        if (column === 'severity') {
          columnKey = `${column}_${severityFormat}`
        }
        const width = getColumnWidth(columnKey)
        if (!isMessageVisible) {
          const inc = currentMessageWidth / visibleColumnsCount
          return width + inc
        }
        return width
    }
  }

  private get rowCharLimit(): number {
    return calculateRowCharWidth(this.state.currentMessageWidth)
  }

  private calculateTotalHeight = (): number => {
    const data = getValuesFromData(this.props.data)

    return data.length * ROW_HEIGHT
  }

  private headerRenderer = ({key, style, columnIndex}) => {
    const column = getColumnFromData(this.props.data, columnIndex)
    const classes = 'logs-viewer--cell logs-viewer--cell-header'

    let columnKey: string = column

    if (column === 'severity') {
      columnKey = this.getSeverityColumn(column)
    }

    return (
      <div className={classes} style={style} key={key}>
        {header(columnKey, this.props.tableColumns)}
      </div>
    )
  }

  private getSeverityColumn(column: string): string {
    const {severityFormat} = this.props
    if (severityFormat === SeverityFormatOptions.dot) {
      return SeverityFormatOptions.dot
    }
    return column
  }

  private getSeverityDotText(text: string): JSX.Element {
    const {severityFormat} = this.props
    if (severityFormat === SeverityFormatOptions.dotText) {
      return <span className="logs-viewer--severity-text">{text}</span>
    }
  }

  private cellRenderer = ({key, style, rowIndex, columnIndex}) => {
    const {severityFormat, severityLevelColors} = this.props

    const column = getColumnFromData(this.props.data, columnIndex)
    const value = getValueFromData(this.props.data, rowIndex, columnIndex)

    let formattedValue: string | JSX.Element
    const isDotNeeded =
      severityFormat === SeverityFormatOptions.dot ||
      severityFormat === SeverityFormatOptions.dotText

    let title: string

    if (column === 'severity' && isDotNeeded) {
      title = value
      const colorLevel = severityLevelColors.find(lc => lc.level === value)
      formattedValue = (
        <>
          <div
            className={`logs-viewer--dot ${value}-severity`}
            title={value}
            onMouseOver={this.handleMouseOver}
            data-index={rowIndex}
            style={this.severityDotStyle(colorLevel.color, colorLevel.level)}
          />
          {this.getSeverityDotText(value)}
        </>
      )
    } else {
      formattedValue = formatColumnValue(column, value, this.rowCharLimit)
      title = formattedValue
    }

    if (column === 'message') {
      formattedValue = <ExpandableMessage formattedValue={formattedValue} />
    }

    const highlightRow = rowIndex === this.state.currentRow

    if (column === 'timestamp') {
      return (
        <div
          className={classnames('logs-viewer--cell', {
            highlight: highlightRow,
          })}
          title={`Jump to '${title}'`}
          key={key}
          style={style}
          data-index={rowIndex}
          onMouseOver={this.handleMouseOver}
        >
          <div
            data-tag-key={column}
            data-tag-value={value}
            onClick={this.handleTimestampClick(`${formattedValue}`)}
            data-index={rowIndex}
            onMouseOver={this.handleMouseOver}
            className="logs-viewer--clickable"
          >
            {formattedValue}
          </div>
        </div>
      )
    }

    if (isClickable(column)) {
      return (
        <div
          className={classnames('logs-viewer--cell', {
            highlight: highlightRow,
          })}
          title={`Filter by '${title}'`}
          key={key}
          style={style}
          data-index={rowIndex}
          onMouseOver={this.handleMouseOver}
        >
          <div
            data-tag-key={column}
            data-tag-value={value}
            onClick={this.handleTagClick}
            data-index={rowIndex}
            onMouseOver={this.handleMouseOver}
            className="logs-viewer--clickable"
          >
            {formattedValue}
          </div>
        </div>
      )
    }

    return (
      <div
        className={classnames(`logs-viewer--cell  ${column}--cell`, {
          highlight: highlightRow,
        })}
        key={key}
        style={style}
        onMouseOver={this.handleMouseOver}
        data-index={rowIndex}
      >
        {formattedValue}
      </div>
    )
  }

  private severityDotStyle = (
    colorName: SeverityColorOptions,
    level: SeverityLevelOptions
  ): CSSProperties => {
    const severityColor = colorForSeverity(colorName, level)
    const brightSeverityColor = color(severityColor)
      .brighter(0.5)
      .hex()

    return {
      background: `linear-gradient(45deg, ${severityColor}, ${brightSeverityColor}`,
    }
  }

  private handleMouseOver = (e: MouseEvent<HTMLElement>): void => {
    const target = e.target as HTMLElement
    const index = target.dataset.index || target.parentElement.dataset.index
    this.setState({currentRow: +index})
  }

  private handleTimestampClick = (time: string) => () => {
    const {onChooseCustomTime} = this.props
    const formattedTime = moment(time, 'YYYY/MM/DD HH:mm:ss').toISOString()
    onChooseCustomTime(formattedTime)
  }

  private handleTagClick = (e: MouseEvent<HTMLElement>) => {
    const {onTagSelection} = this.props
    const target = e.target as HTMLElement

    const selection = {
      tag: target.dataset.tagValue || target.parentElement.dataset.tagValue,
      key: target.dataset.tagKey || target.parentElement.dataset.tagKey,
    }

    onTagSelection(selection)
  }

  private handleMouseOut = () => {
    this.setState({currentRow: -1})
  }

  private get emptyTable(): JSX.Element {
    return (
      <div className="logs-viewer--table-container generic-empty-state">
        <h4>No logs to display</h4>
        <p>
          Try changing the <strong>time range</strong> or{' '}
          <strong>removing filters</strong>
        </p>
      </div>
    )
  }

  private get isTableEmpty(): boolean {
    const rowCount = getDeep(this.props, 'data.values.length', 0)

    return rowCount === 0
  }
}

export default LogsTable
