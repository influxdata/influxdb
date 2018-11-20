// Libraries
import React, {Component, CSSProperties, MouseEvent} from 'react'
import {filter, isEqual} from 'lodash'
import NanoDate from 'nano-date'
import ReactResizeDetector from 'react-resize-detector'
import memoizeOne from 'memoize-one'

// Components
import Dygraphs from 'src/external/dygraph'
import DygraphLegend from 'src/shared/components/DygraphLegend'
import Crosshair from 'src/shared/components/crosshair/Crosshair'

// Utils
import getRange, {getStackedRange} from 'src/shared/parsing/getRangeForDygraph'
import {numberValueFormatter} from 'src/utils/formatting'
import {withHoverTime, InjectedHoverProps} from 'src/dashboards/utils/hoverTime'

// Constants
import {
  AXES_SCALE_OPTIONS,
  DEFAULT_AXIS,
} from 'src/dashboards/constants/cellEditor'
import {
  LINE_COLORS,
  LABEL_WIDTH,
  CHAR_PIXELS,
  barPlotter,
} from 'src/shared/graphs/helpers'
import {getLineColorsHexes} from 'src/shared/constants/graphColorPalettes'
const {LOG, BASE_10, BASE_2} = AXES_SCALE_OPTIONS

import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Axes, TimeRange} from 'src/types'
import {DygraphData, DygraphSeries, Options} from 'src/external/dygraph'
import {Color} from 'src/types/colors'
import {DashboardQuery, ViewType} from 'src/types/v2/dashboards'

const getRangeMemoizedY = memoizeOne(getRange)

const DEFAULT_DYGRAPH_OPTIONS = {
  rightGap: 0,
  yRangePad: 10,
  labelsKMB: true,
  fillGraph: true,
  gridLineWidth: 1,
  axisLineWidth: 2,
  colors: LINE_COLORS,
  axisLabelWidth: 60,
  animatedZooms: true,
  drawAxesAtZero: true,
  highlightCircleSize: 3,
  axisLineColor: '#383846',
  gridLineColor: '#383846',
  labelsSeparateLines: false,
  hideOverlayOnMouseOut: false,
  connectSeparatedPoints: true,
  highlightSeriesBackgroundAlpha: 1.0,
  highlightSeriesBackgroundColor: 'rgb(41, 41, 51)',
}

interface OwnProps {
  type: ViewType
  viewID: string
  queries?: DashboardQuery[]
  timeSeries: DygraphData
  labels: string[]
  options?: Partial<Options>
  dygraphSeries?: DygraphSeries
  colors: Color[]
  timeRange?: TimeRange
  axes?: Axes
  isGraphFilled?: boolean
  onZoom?: (timeRange: TimeRange) => void
  mode?: string
  underlayCallback?: () => void
}

type Props = OwnProps & InjectedHoverProps

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
    underlayCallback: () => {},
    dygraphSeries: {},
    options: {},
  }

  private graphRef: React.RefObject<HTMLDivElement>
  private dygraph: Dygraphs
  private dygraphOptions?: Options

  constructor(props: Props) {
    super(props)

    this.state = {
      xAxisRange: [0, 0],
      isMouseInLegend: false,
    }

    this.graphRef = React.createRef<HTMLDivElement>()
  }

  public componentDidMount() {
    const options = this.collectDygraphOptions()
    const initialOptions = {...DEFAULT_DYGRAPH_OPTIONS, ...options}

    this.dygraph = new Dygraphs(
      this.graphRef.current,
      this.timeSeries,
      initialOptions
    )

    this.dygraphOptions = options
    this.setState({xAxisRange: this.dygraph.xAxisRange()})
  }

  public componentWillUnmount() {
    if (this.dygraph) {
      this.dygraph.destroy()
      delete this.dygraph
    }
  }

  public componentDidUpdate(prevProps: Props) {
    const dygraph = this.dygraph
    const options = this.collectDygraphOptions()
    const optionsChanged = this.haveDygraphOptionsChanged(options)
    const timeRangeChanged = !isEqual(prevProps.timeRange, this.props.timeRange)

    if (optionsChanged) {
      dygraph.updateOptions(options)
      this.dygraphOptions = options
    }

    if (dygraph.isZoomed('x') && timeRangeChanged) {
      dygraph.resetZoom()
    }
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
          id={`graph-ref-${viewID}`}
          className="dygraph-child-container"
          ref={this.graphRef}
          style={this.style}
        >
          <ReactResizeDetector
            resizableElementId={`graph-ref-${viewID}`}
            handleWidth={true}
            handleHeight={true}
            onResize={this.resize}
          />
        </div>
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

  private get style(): CSSProperties {
    return {
      width: 'calc(100% - 32px)',
      height: 'calc(100% - 16px)',
      position: 'absolute',
      top: '8px',
      zIndex: 2,
    }
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

  private get timeSeries() {
    const {timeSeries} = this.props

    // Avoid 'Can't plot empty data set' errors by falling back to a default
    // dataset that's valid for Dygraph.
    return timeSeries && timeSeries.length ? timeSeries : [[0]]
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

  private getYRange = (timeSeries: DygraphData): [number, number] => {
    const {
      options,
      axes: {y},
    } = this.props

    if (options.stackedGraph) {
      return getStackedRange(y.bounds)
    }

    let range = getRangeMemoizedY(timeSeries, y.bounds)

    const [min, max] = range

    // Bug in Dygraph calculates a negative range for logscale when min range is 0
    if (y.scale === LOG && min <= 0) {
      range = [0.01, max]
    }

    return range
  }

  private handleZoom = (lower: number, upper: number) => {
    const {onZoom} = this.props

    if (this.dygraph.isZoomed('x')) {
      return onZoom({
        lower: this.formatTimeRange(lower),
        upper: this.formatTimeRange(upper),
      })
    }

    return onZoom({lower: null, upper: null})
  }

  private handleDraw = () => {
    if (!this.dygraph) {
      return
    }

    const {xAxisRange} = this.state
    const newXAxisRange = this.dygraph.xAxisRange()

    if (!isEqual(xAxisRange, newXAxisRange)) {
      this.setState({xAxisRange: newXAxisRange})
    }
  }

  private formatYVal = (
    yval: number,
    __,
    opts: (name: string) => number
  ): string => {
    const {
      axes: {
        y: {prefix, suffix},
      },
    } = this.props

    return numberValueFormatter(yval, opts, prefix, suffix)
  }

  private eventToTimestamp = ({
    pageX: pxBetweenMouseAndPage,
  }: MouseEvent<HTMLDivElement>): number => {
    const pxBetweenGraphAndPage = this.graphRef.current.getBoundingClientRect()
      .left
    const graphXCoordinate = pxBetweenMouseAndPage - pxBetweenGraphAndPage
    const timestamp = this.dygraph.toDataXCoord(graphXCoordinate)
    const [xRangeStart] = this.dygraph.xAxisRange()
    const clamped = Math.max(xRangeStart, timestamp)

    return clamped
  }

  private handleHideLegend = () => {
    this.setState({isMouseInLegend: false})
    this.props.onSetHoverTime(null)
  }

  private handleShowLegend = (e: MouseEvent<HTMLDivElement>): void => {
    const {isMouseInLegend} = this.state

    if (isMouseInLegend) {
      return
    }

    const newTime = this.eventToTimestamp(e)
    this.props.onSetHoverTime(newTime)
  }

  private collectDygraphOptions(): Options {
    const {
      labels,
      axes: {y},
      type,
      underlayCallback,
      isGraphFilled,
      options: passedOptions,
    } = this.props

    const {
      colorDygraphSeries,
      handleDraw,
      handleZoom,
      timeSeries,
      labelWidth,
      formatYVal,
    } = this

    const options = {
      labels,
      underlayCallback,
      file: timeSeries as any,
      zoomCallback: handleZoom,
      drawCallback: handleDraw,
      fillGraph: isGraphFilled,
      logscale: y.scale === LOG,
      series: colorDygraphSeries,
      plotter: type === ViewType.Bar ? barPlotter : null,
      axes: {
        y: {
          axisLabelWidth: labelWidth,
          labelsKMB: y.base === BASE_10,
          labelsKMG2: y.base === BASE_2,
          axisLabelFormatter: formatYVal,
          valueRange: this.getYRange(timeSeries),
        },
      },
      ...passedOptions,
      // The following options are explicitly coerced to booleans, since
      // dygraphs will not update if they change from `true` to `undefined`
      stepPlot: !!passedOptions.stepPlot,
      stackedGraph: !!passedOptions.stackedGraph,
    }

    return options
  }

  private haveDygraphOptionsChanged(nextOptions: Options): boolean {
    const options = this.dygraphOptions
    const pred = (__, key) => key !== 'file'

    // Peform a deep comparison of the current options and next options, but
    // check the `file` property of each object by reference rather than by
    // logical identity since it can be quite large (it contains all the time
    // series data)
    return (
      !isEqual(filter(options, pred), filter(nextOptions, pred)) ||
      options.file !== nextOptions.file
    )
  }

  private resize = () => {
    if (this.dygraph) {
      this.dygraph.resizeElements_()
      this.dygraph.predraw_()
      this.dygraph.resize()
    }
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

export default withHoverTime(Dygraph)
