// Libraries
import React, {Component} from 'react'
import {get, filter, isEqual} from 'lodash'
import NanoDate from 'nano-date'
import ReactResizeDetector from 'react-resize-detector'
import memoizeOne from 'memoize-one'

// Components
import Dygraphs from 'src/external/dygraph'
import Legend from 'src/shared/components/Legend'
import {ErrorHandling} from 'src/shared/decorators/errors'
import HoverTimeMarker from 'src/shared/components/HoverTimeMarker'

// Utils
import getRange, {getStackedRange} from 'src/shared/parsing/getRangeForDygraph'
import {numberValueFormatter} from 'src/utils/formatting'
import {withHoverTime, InjectedHoverProps} from 'src/dashboards/utils/hoverTime'

// Constants
import {LINE_COLORS, LABEL_WIDTH, CHAR_PIXELS} from 'src/shared/graphs/helpers'
import {getLineColorsHexes} from 'src/shared/constants/graphColorPalettes'
import {
  AXES_SCALE_OPTIONS,
  DEFAULT_AXIS,
} from 'src/dashboards/constants/cellEditor'

// Types
import {Axes, TimeRange} from 'src/types'
import {DygraphData, Options, SeriesLegendData} from 'src/external/dygraph'
import {Color} from 'src/types/colors'
import {DashboardQuery} from 'src/types/v2/dashboards'
import {SeriesDescription} from 'src/shared/parsing/flux/spreadTables'

const getRangeMemoizedY = memoizeOne(getRange)

const {LOG, BASE_10, BASE_2} = AXES_SCALE_OPTIONS

const DEFAULT_DYGRAPH_OPTIONS = {
  yRangePad: 10,
  labelsKMB: true,
  colors: LINE_COLORS,
  animatedZooms: true,
  drawAxesAtZero: true,
  highlightCircleSize: 3,
  axisLineColor: '#383846',
  gridLineColor: '#383846',
  connectSeparatedPoints: true,
}

interface LegendData {
  x: number
  series: SeriesLegendData[]
  xHTML: string
  dygraph: Dygraphs
}

interface OwnProps {
  viewID: string
  queries?: DashboardQuery[]
  timeSeries: DygraphData
  labels: string[]
  seriesDescriptions: SeriesDescription[]
  options?: Partial<Options>
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
  legendData?: {
    time: number
    x: number
    visRect: DOMRect
    values: {[seriesKey: string]: number}
    colors: {[seriesKey: string]: string}
  }
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
    options: {},
  }

  public state: State = {}

  private graphRef: React.RefObject<HTMLDivElement> = React.createRef()
  private dygraph: Dygraphs
  private dygraphOptions?: Options

  public componentDidMount() {
    const options = this.collectDygraphOptions()
    const initialOptions = {
      ...DEFAULT_DYGRAPH_OPTIONS,
      ...options,
      legendFormatter: this.captureLegendData,
    }

    this.dygraph = new Dygraphs(
      this.graphRef.current,
      this.timeSeries,
      initialOptions
    )

    this.dygraphOptions = options
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

    if (optionsChanged) {
      setTimeout(this.resize, 0)
    }
  }

  public render() {
    const {viewID, seriesDescriptions, hoverTime} = this.props
    const {legendData} = this.state

    return (
      <div className="dygraph-child" onMouseLeave={this.handleMouseLeave}>
        {legendData && (
          <Legend {...legendData} seriesDescriptions={seriesDescriptions} />
        )}
        {hoverTime && (
          <HoverTimeMarker x={this.dygraph.toDomXCoord(hoverTime)} />
        )}
        {this.nestedGraph}
        <div
          id={`graph-ref-${viewID}`}
          className="dygraph-child-container"
          ref={this.graphRef}
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

    if (children && children[0]) {
      return children[0]
    } else if (children) {
      return children as JSX.Element
    }

    return null
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

  private get yLabel(): string | null {
    return get(this.props, 'axes.y.label', null)
  }

  private get colors(): string[] {
    const {timeSeries, colors} = this.props
    const numSeries: number = get(timeSeries, '0.length', colors.length)
    const resolvedColors = getLineColorsHexes(colors, numSeries)

    return resolvedColors
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

  private collectDygraphOptions(): Options {
    const {
      labels,
      axes: {y},
      underlayCallback,
      isGraphFilled,
      options: passedOptions,
    } = this.props

    const {
      handleZoom,
      timeSeries,
      labelWidth,
      formatYVal,
      yLabel,
      colors,
    } = this

    const options = {
      labels,
      underlayCallback,
      colors,
      file: timeSeries as any,
      zoomCallback: handleZoom,
      fillGraph: isGraphFilled,
      logscale: y.scale === LOG,
      ylabel: yLabel,
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
      // The following options must be explicitly set to a `null` or boolean
      // value. Otherwise, dygraphs will not update if they change to
      // `undefined`
      stepPlot: !!passedOptions.stepPlot,
      stackedGraph: !!passedOptions.stackedGraph,
      plotter: passedOptions.plotter ? passedOptions.plotter : null,
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

  private handleMouseLeave = () => {
    this.setState({legendData: null})
    this.props.onSetHoverTime(null)
  }

  private captureLegendData = ({x: time, dygraph, series}: LegendData) => {
    if (!time) {
      return ''
    }

    const values = series.reduce(
      (acc, d) => ({
        ...acc,
        [d.label]: d.y,
      }),
      {}
    )

    const colors = series.reduce(
      (acc, d) => ({
        ...acc,
        [d.label]: d.color,
      }),
      {}
    )

    this.setState({
      legendData: {
        time,
        values,
        colors,
        x: dygraph.toDomXCoord(time),
        visRect: this.graphRef.current.getBoundingClientRect() as DOMRect,
      },
    })

    this.props.onSetHoverTime(time)

    return ''
  }
}

export default withHoverTime(Dygraph)
