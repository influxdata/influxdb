import _ from 'lodash'
import moment from 'moment'
import classnames from 'classnames'
import React, {Component, MouseEvent} from 'react'
import {Grid, AutoSizer} from 'react-virtualized'
import {getDeep} from 'src/utils/wrappers'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const ROW_HEIGHT = 26
const CHAR_WIDTH = 9
interface Props {
  data: {
    columns: string[]
    values: string[]
  }
  isScrolledToTop: boolean
  onScrollVertical: () => void
  onScrolledToTop: () => void
  onTagSelection: (selection: {tag: string; key: string}) => void
}

interface State {
  scrollLeft: number
  scrollTop: number
  currentRow: number
}

class LogsTable extends Component<Props, State> {
  public static getDerivedStateFromProps(props, state) {
    const {isScrolledToTop} = props

    let scrollTop = _.get(state, 'scrollTop', 0)
    if (isScrolledToTop) {
      scrollTop = 0
    }

    const scrollLeft = _.get(state, 'scrollLeft', 0)

    return {
      scrollTop,
      scrollLeft,
      currentRow: -1,
    }
  }

  private grid: React.RefObject<Grid>
  private headerGrid: React.RefObject<Grid>
  private currentMessageWidth: number | null

  constructor(props: Props) {
    super(props)

    this.grid = React.createRef()
    this.headerGrid = React.createRef()

    this.state = {
      scrollTop: 0,
      scrollLeft: 0,
      currentRow: -1,
    }
  }

  public componentDidUpdate() {
    this.grid.current.recomputeGridSize()
    this.headerGrid.current.recomputeGridSize()
  }

  public componentDidMount() {
    window.addEventListener('resize', this.handleWindowResize)
  }

  public componentWillUnmount() {
    window.removeEventListener('resize', this.handleWindowResize)
  }

  public render() {
    const rowCount = getDeep(this.props, 'data.values.length', 0)
    const columnCount = getDeep(this.props, 'data.columns.length', 1) - 1

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
                height={height}
                rowHeight={this.calculateRowHeight}
                rowCount={rowCount}
                width={width}
                scrollLeft={this.state.scrollLeft}
                scrollTop={this.state.scrollTop}
                onScroll={this.handleScroll}
                cellRenderer={this.cellRenderer}
                columnCount={columnCount}
                columnWidth={this.getColumnWidth}
                ref={this.grid}
                style={{height: this.calculateTotalHeight()}}
              />
            </FancyScrollbar>
          )}
        </AutoSizer>
      </div>
    )
  }

  private handleWindowResize = () => {
    this.currentMessageWidth = null
    this.grid.current.recomputeGridSize()
    this.headerGrid.current.recomputeGridSize()
  }

  private handleHeaderScroll = ({scrollLeft}) => this.setState({scrollLeft})

  private handleScrollbarScroll = (e: MouseEvent<JSX.Element>) => {
    const {target} = e
    this.handleScroll(target)
  }

  private get widthMapping() {
    return {
      timestamp: 160,
      procid: 80,
      facility: 120,
      severity: 22,
      severity_1: 120,
      host: 300,
    }
  }

  private get messageWidth() {
    if (this.currentMessageWidth) {
      return this.currentMessageWidth
    }

    const columns = getDeep<string[]>(this.props, 'data.columns', [])
    const otherWidth = columns.reduce((acc, col) => {
      if (col === 'message' || col === 'time') {
        return acc
      }

      return acc + _.get(this.widthMapping, col, 200)
    }, 0)

    const calculatedWidth = window.innerWidth - (otherWidth + 180)
    this.currentMessageWidth = Math.max(100 * CHAR_WIDTH, calculatedWidth)

    return this.currentMessageWidth - CHAR_WIDTH
  }

  private getColumnWidth = ({index}: {index: number}) => {
    const column = getDeep<string>(this.props, `data.columns.${index + 1}`, '')

    switch (column) {
      case 'message':
        return this.messageWidth
      default:
        return _.get(this.widthMapping, column, 200)
    }
  }

  private get rowCharLimit(): number {
    return Math.floor(this.messageWidth / CHAR_WIDTH)
  }

  private get columns(): string[] {
    return getDeep<string[]>(this.props, 'data.columns', [])
  }

  private calculateMessageHeight = (index: number): number => {
    const columnIndex = this.columns.indexOf('message')
    const value = getDeep(this.props, `data.values.${index}.${columnIndex}`, '')
    const lines = Math.round(value.length / this.rowCharLimit + 0.25)

    return Math.max(lines, 1) * (ROW_HEIGHT - 14) + 14
  }

  private calculateTotalHeight = (): number => {
    return _.reduce(
      this.props.data.values,
      (acc, __, index) => {
        return acc + this.calculateMessageHeight(index)
      },
      0
    )
  }

  private calculateRowHeight = (d: {index: number}): number => {
    return this.calculateMessageHeight(d.index)
  }

  private handleScroll = scrollInfo => {
    const {scrollLeft, scrollTop} = scrollInfo

    if (scrollTop === 0) {
      this.props.onScrolledToTop()
    } else if (scrollTop !== this.state.scrollTop) {
      this.props.onScrollVertical()
    }

    this.setState({scrollLeft, scrollTop})
  }

  private severityLevel(value: string): string {
    switch (value) {
      case 'emerg':
        return 'Emergency'
      case 'alert':
        return 'Alert'
      case 'crit':
        return 'Critical'
      case 'err':
        return 'Error'
      case 'info':
        return 'Informational'
      default:
        return _.capitalize(value)
    }
  }

  private header(key: string): string {
    return getDeep<string>(
      {
        timestamp: 'Timestamp',
        procid: 'Proc ID',
        message: 'Message',
        appname: 'Application',
        severity: '',
        severity_1: 'Severity',
      },
      key,
      _.capitalize(key)
    )
  }

  private headerRenderer = ({key, style, columnIndex}) => {
    const value = getDeep<string>(
      this.props,
      `data.columns.${columnIndex + 1}`,
      ''
    )

    return (
      <div
        className="logs-viewer--cell logs-viewer--cell-header"
        style={style}
        key={key}
      >
        {this.header(value)}
      </div>
    )
  }

  private cellRenderer = ({key, style, rowIndex, columnIndex}) => {
    const column = getDeep<string>(
      this.props,
      `data.columns.${columnIndex + 1}`,
      ''
    )

    let value: string | JSX.Element = this.props.data.values[rowIndex][
      columnIndex + 1
    ]

    switch (column) {
      case 'timestamp':
        value = moment(+value / 1000000).format('YYYY/MM/DD HH:mm:ss')
        break
      case 'message':
        if (value.indexOf(' ') > this.rowCharLimit - 5) {
          value = _.truncate(value, {length: this.rowCharLimit - 5})
        }
        break
      case 'severity':
        value = (
          <div
            className={`logs-viewer--dot ${value}-severity`}
            title={this.severityLevel(value)}
            onMouseOver={this.handleMouseEnter}
            data-index={rowIndex}
          />
        )
        break
    }

    const highlightRow = rowIndex === this.state.currentRow && columnIndex >= 0

    if (this.isClickable(column)) {
      return (
        <div
          className={classnames(`logs-viewer--cell`, {
            highlight: highlightRow,
          })}
          title={`Filter by "${value}"`}
          style={{...style, padding: '5px'}}
          key={key}
          data-index={rowIndex}
          onMouseOver={this.handleMouseEnter}
        >
          <div
            data-tag-key={column}
            data-tag-value={value}
            onClick={this.handleTagClick}
            data-index={rowIndex}
            onMouseOver={this.handleMouseEnter}
            className="logs-viewer--clickable"
          >
            {value}
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
        onMouseOver={this.handleMouseEnter}
        data-index={rowIndex}
      >
        {value}
      </div>
    )
  }

  private handleMouseEnter = (e: MouseEvent<HTMLElement>): void => {
    const target = e.target as HTMLElement
    this.setState({currentRow: +target.dataset.index})
  }

  private handleTagClick = (e: MouseEvent<HTMLElement>) => {
    const {onTagSelection} = this.props
    const target = e.target as HTMLElement
    const selection = {
      tag: target.dataset.tagValue,
      key: target.dataset.tagKey,
    }

    onTagSelection(selection)
  }

  private handleMouseOut = () => {
    this.setState({currentRow: -1})
  }

  private isClickable(key): boolean {
    return _.includes(
      ['appname', 'facility', 'host', 'hostname', 'severity_1'],
      key
    )
  }
}

export default LogsTable
