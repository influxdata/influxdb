// Libraries
import React, {Component, CSSProperties, MouseEvent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'
import NanoDate from 'nano-date'
import ReactResizeDetector from 'react-resize-detector'

// Components
import D from 'src/external/dygraph'
import DygraphLegend from 'src/shared/components/DygraphLegend'
import StaticLegend from 'src/shared/components/StaticLegend'
import Annotations from 'src/shared/components/Annotations'
import Crosshair from 'src/shared/components/Crosshair'

// Utils
import getRange, {getStackedRange} from 'src/shared/parsing/getRangeForDygraph'
import {getDeep} from 'src/utils/wrappers'
import {numberValueFormatter} from 'src/utils/formatting'

// Constants
import {
  AXES_SCALE_OPTIONS,
  DEFAULT_AXIS,
} from 'src/dashboards/constants/cellEditor'
import {buildDefaultYLabel} from 'src/shared/presenters'
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
import {
  Axes,
  Query,
  CellType,
  TimeRange,
  DygraphData,
  DygraphClass,
  DygraphSeries,
  Constructable,
} from 'src/types'
import {LineColor} from 'src/types/colors'

const Dygraphs = D as Constructable<DygraphClass>

interface Props {
  type: CellType
  cellID: string
  queries: Query[]
  timeSeries: DygraphData
  labels: string[]
  options: dygraphs.Options
  containerStyle: object // TODO
  dygraphSeries: DygraphSeries
  timeRange: TimeRange
  colors: LineColor[]
  handleSetHoverTime: (t: string) => void
  axes?: Axes
  isGraphFilled?: boolean
  staticLegend?: boolean
  setResolution?: (w: number) => void
  onZoom?: (timeRange: TimeRange) => void
  mode?: string
  underlayCallback?: () => void
}

interface State {
  staticLegendHeight: number
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
    containerStyle: {},
    isGraphFilled: true,
    onZoom: () => {},
    staticLegend: false,
    setResolution: () => {},
    handleSetHoverTime: () => {},
    underlayCallback: () => {},
  }

  private graphRef: React.RefObject<HTMLDivElement>
  private dygraph: DygraphClass

  constructor(props: Props) {
    super(props)
    this.state = {
      staticLegendHeight: 0,
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

    let defaultOptions = {
      ...options,
      labels,
      fillGraph,
      file: this.timeSeries,
      ylabel: this.getLabel('y'),
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

    if (type === CellType.Bar) {
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

    const timeSeries = this.timeSeries

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
      ylabel: this.getLabel('y'),
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
      plotter: type === CellType.Bar ? barPlotter : null,
      underlayCallback,
    }

    dygraph.updateOptions(updateOptions)

    const {w} = this.dygraph.getArea()
    this.props.setResolution(w)
    this.resize()
  }

  public render() {
    const {staticLegendHeight, xAxisRange} = this.state
    const {staticLegend, cellID} = this.props

    return (
      <div
        className="dygraph-child"
        onMouseMove={this.handleShowLegend}
        onMouseLeave={this.handleHideLegend}
      >
        {this.dygraph && (
          <div className="dygraph-addons">
            {this.areAnnotationsVisible && (
              <Annotations
                dygraph={this.dygraph}
                dWidth={this.dygraph.width_}
                staticLegendHeight={staticLegendHeight}
                xAxisRange={xAxisRange}
              />
            )}
            <DygraphLegend
              cellID={cellID}
              dygraph={this.dygraph}
              onHide={this.handleHideLegend}
              onShow={this.handleShowLegend}
              onMouseEnter={this.handleMouseEnterLegend}
            />
            <Crosshair
              dygraph={this.dygraph}
              staticLegendHeight={staticLegendHeight}
            />
          </div>
        )}
        {staticLegend && (
          <StaticLegend
            dygraphSeries={this.colorDygraphSeries}
            dygraph={this.dygraph}
            height={staticLegendHeight}
            onUpdateHeight={this.handleUpdateStaticLegendHeight}
          />
        )}
        {this.nestedGraph &&
          React.cloneElement(this.nestedGraph, {
            staticLegendHeight,
          })}
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

  private get dygraphStyle(): CSSProperties {
    const {containerStyle, staticLegend} = this.props
    const {staticLegendHeight} = this.state

    if (staticLegend) {
      const cellVerticalPadding = 16

      return {
        ...containerStyle,
        zIndex: 2,
        height: `calc(100% - ${staticLegendHeight + cellVerticalPadding}px)`,
      }
    }

    return {...containerStyle, zIndex: 2}
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
  }: MouseEvent<HTMLDivElement>): string => {
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

  private handleShowLegend = (e: MouseEvent<HTMLDivElement>): void => {
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

  private get timeSeries() {
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

  private get areAnnotationsVisible() {
    return !!this.dygraph
  }

  private getLabel = (axis: string): string => {
    const {axes, queries} = this.props
    const label = getDeep<string>(axes, `${axis}.label`, '')
    const queryConfig = _.get(queries, ['0', 'queryConfig'], false)

    if (label || !queryConfig) {
      return label
    }

    return buildDefaultYLabel(queryConfig)
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

  private handleUpdateStaticLegendHeight = (staticLegendHeight: number) => {
    this.setState({staticLegendHeight})
  }

  private handleMouseEnterLegend = () => {
    this.setState({isMouseInLegend: true})
  }
}

const mapStateToProps = ({annotations: {mode}}) => ({
  mode,
})

export default connect(mapStateToProps, null)(Dygraph)
