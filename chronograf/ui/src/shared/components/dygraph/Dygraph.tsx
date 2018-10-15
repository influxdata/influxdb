// Libraries
import React, {Component, CSSProperties, MouseEvent} from 'react'
import _ from 'lodash'
import NanoDate from 'nano-date'
import ReactResizeDetector from 'react-resize-detector'

// Components
import Dygraphs from 'src/external/dygraph'
import DygraphLegend from 'src/shared/components/DygraphLegend'
import Crosshair from 'src/shared/components/crosshair/Crosshair'

// Utils
import getRange, {getStackedRange} from 'src/shared/parsing/getRangeForDygraph'
import {numberValueFormatter} from 'src/utils/formatting'

// Constants
import {
  AXES_SCALE_OPTIONS,
  DEFAULT_AXIS,
} from 'src/dashboards/constants/cellEditor'
import {NULL_HOVER_TIME} from 'src/shared/constants/tableGraph'
import {
  OPTIONS,
  LINE_COLORS,
  LABEL_WIDTH,
  CHAR_PIXELS,
  barPlotter,
} from 'src/shared/graphs/helpers'
import {getLineColorsHexes} from 'src/shared/constants/graphColorPalettes'
const {LOG, BASE_10, BASE_2} = AXES_SCALE_OPTIONS

import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Color} from 'src/types/colors'
import {Axes, TimeRange} from 'src/types'
import {DashboardQuery, ViewType} from 'src/types/v2/dashboards'
import {DygraphData, DygraphSeries, Options} from 'src/external/dygraph'

interface Props {
  type: ViewType
  timeSeries: DygraphData
  labels: string[]
  options: Partial<Options>
  colors: Color[]
  handleSetHoverTime: (t: string) => void
  viewID?: string
  axes?: Axes
  mode?: string
  queries?: DashboardQuery[]
  timeRange?: TimeRange
  dygraphSeries?: DygraphSeries
  underlayCallback?: () => void
  setResolution?: (w: number) => void
  onZoom?: (timeRange: TimeRange) => void
  isGraphFilled?: boolean
}

interface State {
  xAxisRange: [number, number]
  isMouseInLegend: boolean
}

@ErrorHandling
class Dygraph extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    axes: {
      x: {
        bounds: [null, null],
        ...DEFAULT_AXIS,
      },
      y: {
        bounds: [null, null],
        ...DEFAULT_AXIS,
      },
      y2: {
        bounds: undefined,
        ...DEFAULT_AXIS,
      },
    },
    isGraphFilled: true,
    onZoom: () => {},
    setResolution: () => {},
    handleSetHoverTime: () => {},
    underlayCallback: () => {},
    dygraphSeries: {},
  }

  private graphRef: React.RefObject<HTMLDivElement>
  private dygraph: Dygraphs

  constructor(props: Props) {
    super(props)
    this.state = {
      xAxisRange: [0, 0],
      isMouseInLegend: false,
    }

    this.graphRef = React.createRef<HTMLDivElement>()
  }

  public componentDidMount() {
    const {
      type,
      options,
      labels,
      axes: {y, y2},
      isGraphFilled: fillGraph,
      underlayCallback,
    } = this.props

    const timeSeries = this.timeSeries

    let defaultOptions: Partial<Options> = {
      ...options,
      labels,
      fillGraph,
      file: this.timeSeries,
      logscale: y.scale === LOG,
      colors: LINE_COLORS,
      series: this.colorDygraphSeries,
      axes: {
        y: {
          valueRange: this.getYRange(timeSeries),
          axisLabelFormatter: (
            yval: number,
            __,
            opts: (name: string) => number
          ) => numberValueFormatter(yval, opts, y.prefix, y.suffix),
          axisLabelWidth: this.labelWidth,
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
        },
        y2: {
          valueRange: getRange(timeSeries, y2.bounds),
        },
      },
      zoomCallback: (lower: number, upper: number) =>
        this.handleZoom(lower, upper),
      drawCallback: () => this.handleDraw(),
      underlayCallback,
      highlightCircleSize: 3,
    }

    if (type === ViewType.Bar) {
      defaultOptions = {
        ...defaultOptions,
        plotter: barPlotter,
      }
    }

    this.dygraph = new Dygraphs(this.graphRef.current, timeSeries, {
      ...defaultOptions,
      ...OPTIONS,
      ...options,
    })

    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)
    this.setState({xAxisRange: this.dygraph.xAxisRange()})
  }

  public componentWillUnmount() {
    if (this.dygraph) {
      this.dygraph.destroy()
      delete this.dygraph
    }
  }

  public componentDidUpdate(prevProps: Props) {
    const {
      labels,
      axes: {y, y2},
      options,
      type,
      underlayCallback,
    } = this.props

    const dygraph = this.dygraph

    if (!dygraph) {
      throw new Error(
        'Dygraph not configured in time; this should not be possible!'
      )
    }

    const timeSeries: DygraphData = this.timeSeries

    const timeRangeChanged = !_.isEqual(
      prevProps.timeRange,
      this.props.timeRange
    )

    if (this.dygraph.isZoomed() && timeRangeChanged) {
      this.dygraph.resetZoom()
    }

    const updateOptions = {
      ...options,
      labels,
      file: timeSeries,
      logscale: y.scale === LOG,
      axes: {
        y: {
          valueRange: this.getYRange(timeSeries),
          axisLabelFormatter: (
            yval: number,
            __,
            opts: (name: string) => number
          ) => numberValueFormatter(yval, opts, y.prefix, y.suffix),
          axisLabelWidth: this.labelWidth,
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
        },
        y2: {
          valueRange: getRange(timeSeries, y2.bounds),
        },
      },
      colors: LINE_COLORS,
      series: this.colorDygraphSeries,
      plotter: type === ViewType.Bar ? barPlotter : null,
      underlayCallback,
    }

    dygraph.updateOptions(updateOptions)

    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)
    this.resize()
  }

  public render() {
    const {viewID} = this.props

    return (
      <div
        className="dygraph-child"
        onMouseMove={this.handleShowLegend}
        onMouseLeave={this.handleHideLegend}
      >
        {this.dygraph && (
          <div className="dygraph-addons">
            <DygraphLegend
              viewID={viewID}
              dygraph={this.dygraph}
              onHide={this.handleHideLegend}
              onShow={this.handleShowLegend}
              onMouseEnter={this.handleMouseEnterLegend}
            />
            <Crosshair dygraph={this.dygraph} />
          </div>
        )}
        {this.nestedGraph && React.cloneElement(this.nestedGraph)}
        <div
          className="dygraph-child-container"
          ref={this.graphRef}
          style={this.dygraphStyle}
        />
        <ReactResizeDetector
          handleWidth={true}
          handleHeight={true}
          onResize={this.resize}
        />
      </div>
    )
  }

  private get nestedGraph(): JSX.Element {
    const {children} = this.props
    if (children) {
      if (children[0]) {
        return children[0]
      }

      return children as JSX.Element
    }

    return null
  }

  private get containerStyle(): CSSProperties {
    return {
      width: 'calc(100% - 32px)',
      height: 'calc(100% - 16px)',
      position: 'absolute',
      top: '8px',
    }
  }

  private get dygraphStyle(): CSSProperties {
    return {...this.containerStyle, zIndex: 2}
  }

  private getYRange = (timeSeries: DygraphData): [number, number] => {
    const {
      options,
      axes: {y},
    } = this.props

    if (options.stackedGraph) {
      return getStackedRange(y.bounds)
    }

    const range = getRange(timeSeries, y.bounds)
    const [min, max] = range

    // Bug in Dygraph calculates a negative range for logscale when min range is 0
    if (y.scale === LOG && timeSeries.length === 1 && min <= 0) {
      return [0.1, max]
    }

    return range
  }

  private handleZoom = (lower: number, upper: number) => {
    const {onZoom} = this.props

    if (this.dygraph.isZoomed() === false) {
      return onZoom({lower: null, upper: null})
    }

    onZoom({
      lower: this.formatTimeRange(lower),
      upper: this.formatTimeRange(upper),
    })
  }

  private handleDraw = () => {
    if (!this.dygraph) {
      return
    }

    const {xAxisRange} = this.state
    const newXAxisRange = this.dygraph.xAxisRange()

    if (!_.isEqual(xAxisRange, newXAxisRange)) {
      this.setState({xAxisRange: newXAxisRange})
    }
  }

  private eventToTimestamp = ({
    pageX: pxBetweenMouseAndPage,
  }: MouseEvent<Element>): string => {
    const {
      left: pxBetweenGraphAndPage,
    } = this.graphRef.current.getBoundingClientRect()
    const graphXCoordinate = pxBetweenMouseAndPage - pxBetweenGraphAndPage
    const timestamp = this.dygraph.toDataXCoord(graphXCoordinate)
    const [xRangeStart] = this.dygraph.xAxisRange()
    const clamped = Math.max(xRangeStart, timestamp)
    return `${clamped}`
  }

  private handleHideLegend = () => {
    this.setState({isMouseInLegend: false})
    this.props.handleSetHoverTime(NULL_HOVER_TIME)
  }

  private handleShowLegend = (e: MouseEvent<Element>): void => {
    const {isMouseInLegend} = this.state

    if (isMouseInLegend) {
      return
    }

    const newTime = this.eventToTimestamp(e)
    this.props.handleSetHoverTime(newTime)
  }

  private get labelWidth() {
    const {
      axes: {y},
    } = this.props

    return (
      LABEL_WIDTH +
      y.prefix.length * CHAR_PIXELS +
      y.suffix.length * CHAR_PIXELS
    )
  }

  private get timeSeries(): DygraphData {
    const {timeSeries} = this.props
    // Avoid 'Can't plot empty data set' errors by falling back to a
    // default dataset that's valid for Dygraph.
    return timeSeries.length ? timeSeries : [[0]]
  }

  private get colorDygraphSeries() {
    const {dygraphSeries, colors} = this.props
    const numSeries = Object.keys(dygraphSeries).length
    const dygraphSeriesKeys = Object.keys(dygraphSeries).sort()
    const lineColors = getLineColorsHexes(colors, numSeries)

    const coloredDygraphSeries = {}
    for (const seriesName in dygraphSeries) {
      if (dygraphSeries.hasOwnProperty(seriesName)) {
        const series = dygraphSeries[seriesName]
        const color = lineColors[dygraphSeriesKeys.indexOf(seriesName)]
        coloredDygraphSeries[seriesName] = {...series, color}
      }
    }
    return coloredDygraphSeries
  }

  private resize = () => {
    this.dygraph.resizeElements_()
    this.dygraph.predraw_()
    this.dygraph.resize()
  }

  private formatTimeRange = (date: number): string => {
    if (!date) {
      return ''
    }
    const nanoDate = new NanoDate(date)
    return nanoDate.toISOString()
  }

  private handleMouseEnterLegend = () => {
    this.setState({isMouseInLegend: true})
  }
}

export default Dygraph
