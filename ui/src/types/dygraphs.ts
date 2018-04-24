export type DataArray = number[][]

export type Data = string | DataArray | google.visualization.DataTable

export interface PerSeriesOptions {
  /**
   * Set to either 'y1' or 'y2' to assign a series to a y-axis (primary or secondary). Must be
   * set per-series.
   */
  axis?: 'y1' | 'y2'

  /**
   * A per-series color definition. Used in conjunction with, and overrides, the colors option.
   */
  color?: string

  /**
   * Draw a small dot at each point, in addition to a line going through the point. This makes
   * the individual data points easier to see, but can increase visual clutter in the chart.
   * The small dot can be replaced with a custom rendering by supplying a <a
   * href='#drawPointCallback'>drawPointCallback</a>.
   */
  drawPoints?: boolean

  /**
   * Error bars (or custom bars) for each series are drawn in the same color as the series, but
   * with partial transparency. This sets the transparency. A value of 0.0 means that the error
   * bars will not be drawn, whereas a value of 1.0 means that the error bars will be as dark
   * as the line for the series itself. This can be used to produce chart lines whose thickness
   * varies at each point.
   */
  fillAlpha?: number

  /**
   * Should the area underneath the graph be filled? This option is not compatible with error
   * bars. This may be set on a <a href='per-axis.html'>per-series</a> basis.
   */
  fillGraph?: boolean

  /**
   * The size in pixels of the dot drawn over highlighted points.
   */
  highlightCircleSize?: number

  /**
   * The size of the dot to draw on each point in pixels (see drawPoints). A dot is always
   * drawn when a point is "isolated", i.e. there is a missing point on either side of it. This
   * also controls the size of those dots.
   */
  pointSize?: number

  /**
   * Mark this series for inclusion in the range selector. The mini plot curve will be an
   * average of all such series. If this is not specified for any series, the default behavior
   * is to average all the series. Setting it for one series will result in that series being
   * charted alone in the range selector.
   */
  showInRangeSelector?: boolean

  /**
   * When set, display the graph as a step plot instead of a line plot. This option may either
   * be set for the whole graph or for single series.
   */
  stepPlot?: boolean

  /**
   * Draw a border around graph lines to make crossing lines more easily distinguishable.
   * Useful for graphs with many lines.
   */
  strokeBorderWidth?: number

  /**
   * Color for the line border used if strokeBorderWidth is set.
   */
  strokeBorderColor?: string

  /**
   * A custom pattern array where the even index is a draw and odd is a space in pixels. If
   * null then it draws a solid line. The array should have a even length as any odd lengthed
   * array could be expressed as a smaller even length array. This is used to create dashed
   * lines.
   */
  strokePattern?: number[]

  /**
   * The width of the lines connecting data points. This can be used to increase the contrast
   * or some graphs.
   */
  strokeWidth?: number
}

export interface PerAxisOptions {
  /**
   * Color for x- and y-axis labels. This is a CSS color string.
   */
  axisLabelColor?: string

  /**
   * Size of the font (in pixels) to use in the axis labels, both x- and y-axis.
   */
  axisLabelFontSize?: number

  /**
   * Function to call to format the tick values that appear along an axis. This is usually set
   * on a <a href='per-axis.html'>per-axis</a> basis.
   */
  axisLabelFormatter?: (
    v: number | Date,
    granularity: number,
    opts: (name: string) => any,
    dygraph: Dygraph
  ) => any

  /**
   * Width (in pixels) of the containing divs for x- and y-axis labels. For the y-axis, this
   * also controls the width of the y-axis. Note that for the x-axis, this is independent from
   * pixelsPerLabel, which controls the spacing between labels.
   */
  axisLabelWidth?: number

  /**
   * Color of the x- and y-axis lines. Accepts any value which the HTML canvas strokeStyle
   * attribute understands, e.g. 'black' or 'rgb(0, 100, 255)'.
   */
  axisLineColor?: string

  /**
   * Thickness (in pixels) of the x- and y-axis lines.
   */
  axisLineWidth?: number

  /**
   * The size of the line to display next to each tick mark on x- or y-axes.
   */
  axisTickSize?: number

  /**
   * Whether to draw the specified axis. This may be set on a per-axis basis to define the
   * visibility of each axis separately. Setting this to false also prevents axis ticks from
   * being drawn and reclaims the space for the chart grid/lines.
   */
  drawAxis?: boolean

  /**
   * The color of the gridlines. This may be set on a per-axis basis to define each axis' grid
   * separately.
   */
  gridLineColor?: string

  /**
   * A custom pattern array where the even index is a draw and odd is a space in pixels. If
   * null then it draws a solid line. The array should have a even length as any odd lengthed
   * array could be expressed as a smaller even length array. This is used to create dashed
   * gridlines.
   */
  gridLinePattern?: number[]

  /**
   * Thickness (in pixels) of the gridlines drawn under the chart. The vertical/horizontal
   * gridlines can be turned off entirely by using the drawXGrid and drawYGrid options. This
   * may be set on a per-axis basis to define each axis' grid separately.
   */
  gridLineWidth?: number

  /**
   * Only valid for y and y2, has no effect on x: This option defines whether the y axes should
   * align their ticks or if they should be independent. Possible combinations: 1.) y=true,
   * y2=false (default): y is the primary axis and the y2 ticks are aligned to the the ones of
   * y. (only 1 grid) 2.) y=false, y2=true: y2 is the primary axis and the y ticks are aligned
   * to the the ones of y2. (only 1 grid) 3.) y=true, y2=true: Both axis are independent and
   * have their own ticks. (2 grids) 4.) y=false, y2=false: Invalid configuration causes an
   * error.
   */
  independentTicks?: boolean

  /**
   * When set for the y-axis or x-axis, the graph shows that axis in log scale. Any values less
   * than or equal to zero are not displayed. Showing log scale with ranges that go below zero
   * will result in an unviewable graph.
   *
   * Not compatible with showZero. connectSeparatedPoints is ignored. This is ignored for
   * date-based x-axes.
   */
  logscale?: boolean

  /**
   * When displaying numbers in normal (not scientific) mode, large numbers will be displayed
   * with many trailing zeros (e.g. 100000000 instead of 1e9). This can lead to unwieldy y-axis
   * labels. If there are more than <code>maxNumberWidth</code> digits to the left of the
   * decimal in a number, dygraphs will switch to scientific notation, even when not operating
   * in scientific mode. If you'd like to see all those digits, set this to something large,
   * like 20 or 30.
   */
  maxNumberWidth?: number

  /**
   * Number of pixels to require between each x- and y-label. Larger values will yield a
   * sparser axis with fewer ticks. This is set on a <a href='per-axis.html'>per-axis</a>
   * basis.
   */
  pixelsPerLabel?: number

  /**
   * By default, dygraphs displays numbers with a fixed number of digits after the decimal
   * point. If you'd prefer to have a fixed number of significant figures, set this option to
   * that number of sig figs. A value of 2, for instance, would cause 1 to be display as 1.0
   * and 1234 to be displayed as 1.23e+3.
   */
  sigFigs?: number

  /**
   * This lets you specify an arbitrary function to generate tick marks on an axis. The tick
   * marks are an array of (value, label) pairs. The built-in functions go to great lengths to
   * choose good tick marks so, if you set this option, you'll most likely want to call one of
   * them and modify the result. See dygraph-tickers.js for an extensive discussion. This is
   * set on a <a href='per-axis.html'>per-axis</a> basis.
   */
  ticker?: (
    min: number,
    max: number,
    pixels: number,
    opts: (name: string) => any,
    dygraph: Dygraph,
    vals: number[]
  ) => Array<{v: number; label: string}>

  /**
   * Function to provide a custom display format for the values displayed on mouseover. This
   * does not affect the values that appear on tick marks next to the axes. To format those,
   * see axisLabelFormatter. This is usually set on a <a href='per-axis.html'>per-axis</a>
   * basis.
   */
  valueFormatter?: (
    v: number,
    opts: (name: string) => any,
    seriesName: string,
    dygraph: Dygraph,
    row: number,
    col: number
  ) => any

  /**
   * Explicitly set the vertical range of the graph to [low, high]. This may be set on a
   * per-axis basis to define each y-axis separately. If either limit is unspecified, it will
   * be calculated automatically (e.g. [null, 30] to automatically calculate just the lower
   * bound)
   */
  valueRange?: number[]

  /**
   * Whether to display gridlines in the chart. This may be set on a per-axis basis to define
   * the visibility of each axis' grid separately.
   */
  drawGrid?: boolean

  /**
   * Show K/M/B for thousands/millions/billions on y-axis.
   */
  labelsKMB?: boolean

  /**
   * Show k/M/G for kilo/Mega/Giga on y-axis. This is different than <code>labelsKMB</code> in
   * that it uses base 2, not 10.
   */
  labelsKMG2?: boolean
}

export interface SeriesLegendData {
  /**
   * Assigned or generated series color
   */
  color: string
  /**
   * Series line dash
   */
  dashHTML: string
  /**
   * Whether currently focused or not
   */
  isHighlighted: boolean
  /**
   * Whether the series line is inside the selected/zoomed region
   */
  isVisible: boolean
  /**
   * Assigned label to this series
   */
  label: string
  /**
   * Generated label html for this series
   */
  labelHTML: string
  /**
   * y value of this series
   */
  y: number
  /**
   * Generated html for y value
   */
  yHTML: string
}

export interface LegendData {
  /**
   * x value of highlighted points
   */
  x: number
  /**
   * Generated HTML for x value
   */
  xHTML: string
  /**
   * Series data for the highlighted points
   */
  series: SeriesLegendData[]
  /**
   * Dygraph object for this graph
   */
  dygraph: Dygraph
}

export interface SeriesProperties {
  name: string
  column: number
  visible: boolean
  color: string
  axis: number
}

export interface Area {
  x: number
  y: number
  w: number
  h: number
}

/**
 * Point structure.
 *
 * xval_* and yval_* are the original unscaled data values,
 * while x_* and y_* are scaled to the range (0.0-1.0) for plotting.
 * yval_stacked is the cumulative Y value used for stacking graphs,
 * and bottom/top/minus/plus are used for error bar graphs.
 */
export interface Point {
  idx: number
  name: string
  x?: number
  xval?: number
  y_bottom?: number
  y?: number
  y_stacked?: number
  y_top?: number
  yval_minus?: number
  yval?: number
  yval_plus?: number
  yval_stacked?: number
  annotation?: dygraphs.Annotation
}

export interface Annotation {
  /** The name of the series to which the annotated point belongs. */
  series: string

  /**
   * The x value of the point. This should be the same as the value
   * you specified in your CSV file, e.g. "2010-09-13".
   * You must set either x or xval.
   */
  x?: number | string

  /**
   * numeric value of the point, or millis since epoch.
   */
  xval?: number

  /**	Text that will appear on the annotation's flag. */
  shortText?: string

  /** A longer description of the annotation which will appear when the user hovers over it. */
  text?: string

  /**
   * Specify in place of shortText to mark the annotation with an image rather than text.
   * If you specify this, you must specify width and height.
   */
  icon?: string

  /**	Width (in pixels) of the annotation flag or icon. */
  width?: number
  /** Height (in pixels) of the annotation flag or icon. */
  height?: number

  /**	CSS class to use for styling the annotation. */
  cssClass?: string

  /**	Height of the tick mark (in pixels) connecting the point to its flag or icon. */
  tickHeight?: number

  /**	If true, attach annotations to the x-axis, rather than to actual points. */
  attachAtBottom?: boolean

  div?: HTMLDivElement

  /** This function is called whenever the user clicks on this annotation. */
  clickHandler?: (
    annotation: dygraphs.Annotation,
    point: Point,
    dygraph: Dygraph,
    event: MouseEvent
  ) => any

  /** This function is called whenever the user mouses over this annotation. */
  mouseOverHandler?: (
    annotation: dygraphs.Annotation,
    point: Point,
    dygraph: Dygraph,
    event: MouseEvent
  ) => any

  /** This function is called whenever the user mouses out of this annotation. */
  mouseOutHandler?: (
    annotation: dygraphs.Annotation,
    point: Point,
    dygraph: Dygraph,
    event: MouseEvent
  ) => any

  /** this function is called whenever the user double-clicks on this annotation. */
  dblClickHandler?: (
    annotation: dygraphs.Annotation,
    point: Point,
    dygraph: Dygraph,
    event: MouseEvent
  ) => any
}

export type Axis = 'x' | 'y' | 'y2'
