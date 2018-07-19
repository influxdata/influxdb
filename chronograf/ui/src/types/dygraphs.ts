export type DygraphData = number[][]

export type Data = string | DygraphData | google.visualization.DataTable

export type DygraphValue = string | number | Date | null

export interface DygraphAxis {
  bounds: [number, number]
  label: string
  prefix: string
  suffix: string
  base: string
  scale: string
}

export interface DygraphSeries {
  [x: string]: {
    axis: string
  }
}

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

  /*	Text that will appear on the annotation's flag. */
  shortText?: string

  /** A longer description of the annotation which will appear when the user hovers over it. */
  text?: string

  /**
   * Specify in place of shortText to mark the annotation with an image rather than text.
   * If you specify this, you must specify width and height.
   */
  icon?: string

  /*	Width (in pixels) of the annotation flag or icon. */
  width?: number

  /** Height (in pixels) of the annotation flag or icon. */
  height?: number

  /*	CSS class to use for styling the annotation. */
  cssClass?: string

  /*	Height of the tick mark (in pixels) connecting the point to its flag or icon. */
  tickHeight?: number

  /*	If true, attach annotations to the x-axis, rather than to actual points. */
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

export declare class DygraphClass {
  // Tick granularities (passed to ticker).
  public static SECONDLY: number
  public static TWO_SECONDLY: number
  public static FIVE_SECONDLY: number
  public static TEN_SECONDLY: number
  public static THIRTY_SECONDLY: number
  public static MINUTELY: number
  public static TWO_MINUTELY: number
  public static FIVE_MINUTELY: number
  public static TEN_MINUTELY: number
  public static THIRTY_MINUTELY: number
  public static HOURLY: number
  public static TWO_HOURLY: number
  public static SIX_HOURLY: number
  public static DAILY: number
  public static TWO_DAILY: number
  public static WEEKLY: number
  public static MONTHLY: number
  public static QUARTERLY: number
  public static BIANNUAL: number
  public static ANNUAL: number
  public static DECADAL: number
  public static CENTENNIAL: number
  public static NUM_GRANULARITIES: number

  public static defaultInteractionModel: any

  public static DOTTED_LINE: number[]
  public static DASHED_LINE: number[]
  public static DOT_DASH_LINE: number[]

  public static Plotters: {
    errorPlotter: any
    linePlotter: any
    fillPlotter: any
  }

  // tslint:disable-next-line:variable-name
  public width_: number
  public graphDiv: HTMLElement

  constructor(
    container: HTMLElement | string,
    data: dygraphs.Data | (() => dygraphs.Data),
    options?: dygraphs.Options
  )

  /**
   * Returns the zoomed status of the chart for one or both axes.
   *
   * Axis is an optional parameter. Can be set to 'x' or 'y'.
   *
   * The zoomed status for an axis is set whenever a user zooms using the mouse
   * or when the dateWindow or valueRange are updated (unless the
   * isZoomedIgnoreProgrammaticZoom option is also specified).
   */
  public isZoomed(axis?: 'x' | 'y'): boolean

  public predraw_(): () => void

  public resizeElements_(): () => void

  /**
   * Returns information about the Dygraph object, including its containing ID.
   */
  public toString(): string

  /**
   * Returns the current value for an option, as set in the constructor or via
   * updateOptions. You may pass in an (optional) series name to get per-series
   * values for the option.
   *
   * All values returned by this method should be considered immutable. If you
   * modify them, there is no guarantee that the changes will be honored or that
   * dygraphs will remain in a consistent state. If you want to modify an option,
   * use updateOptions() instead.
   *
   * @param {string} name The name of the option (e.g. 'strokeWidth')
   * @param {string=} opt_seriesName Series name to get per-series values.
   * @return {*} The value of the option.
   */
  public getOption(name: string, seriesName?: string): any

  /**
   * Get the value of an option on a per-axis basis.
   */
  public getOptionForAxis(name: string, axis: dygraphs.Axis): any

  /**
   * Returns the current rolling period, as set by the user or an option.
   */
  public rollPeriod(): number

  /**
   * Returns the currently-visible x-range. This can be affected by zooming,
   * panning or a call to updateOptions.
   * Returns a two-element array: [left, right].
   * If the Dygraph has dates on the x-axis, these will be millis since epoch.
   */
  public xAxisRange(): [number, number]

  /**
   * Returns the lower- and upper-bound x-axis values of the data set.
   */
  public xAxisExtremes(): [number, number]

  /**
   * Returns the currently-visible y-range for an axis. This can be affected by
   * zooming, panning or a call to updateOptions. Axis indices are zero-based. If
   * called with no arguments, returns the range of the first axis.
   * Returns a two-element array: [bottom, top].
   */
  public yAxisRange(idx?: number): [number, number]

  /**
   * Returns the currently-visible y-ranges for each axis. This can be affected by
   * zooming, panning, calls to updateOptions, etc.
   * Returns an array of [bottom, top] pairs, one for each y-axis.
   */
  public yAxisRanges(): Array<[number, number]>

  /**
   * Convert from data coordinates to canvas/div X/Y coordinates.
   * If specified, do this conversion for the coordinate system of a particular
   * axis. Uses the first axis by default.
   * Returns a two-element array: [X, Y]
   *
   * Note: use toDomXCoord instead of toDomCoords(x, null) and use toDomYCoord
   * instead of toDomCoords(null, y, axis).
   */
  public toDomCoords(x: number, y: number, axis?: number): [number, number]

  /**
   * Convert from data x coordinates to canvas/div X coordinate.
   * If specified, do this conversion for the coordinate system of a particular
   * axis.
   * Returns a single value or null if x is null.
   */
  public toDomXCoord(x: number): number

  /**
   * Convert from data y coordinates to canvas/div Y coordinate and optional
   * axis. Uses the first axis by default.
   *
   * returns a single value or null if y is null.
   */
  public toDomYCoord(y: number, axis?: number): number

  /**
   * Convert from canvas/div coords to data coordinates.
   * If specified, do this conversion for the coordinate system of a particular
   * axis. Uses the first axis by default.
   * Returns a two-element array: [X, Y].
   *
   * Note: use toDataXCoord instead of toDataCoords(x, null) and use toDataYCoord
   * instead of toDataCoords(null, y, axis).
   */
  public toDataCoords(x: number, y: number, axis?: number): [number, number]

  /**
   * Convert from canvas/div x coordinate to data coordinate.
   *
   * If x is null, this returns null.
   */
  public toDataXCoord(x: number): number

  /**
   * Convert from canvas/div y coord to value.
   *
   * If y is null, this returns null.
   * if axis is null, this uses the first axis.
   */
  public toDataYCoord(y: number, axis?: number): number

  /**
   * Converts a y for an axis to a percentage from the top to the
   * bottom of the drawing area.
   *
   * If the coordinate represents a value visible on the canvas, then
   * the value will be between 0 and 1, where 0 is the top of the canvas.
   * However, this method will return values outside the range, as
   * values can fall outside the canvas.
   *
   * If y is null, this returns null.
   * if axis is null, this uses the first axis.
   *
   * @param {number} y The data y-coordinate.
   * @param {number} [axis] The axis number on which the data coordinate lives.
   * @return {number} A fraction in [0, 1] where 0 = the top edge.
   */
  public toPercentYCoord(y: number, axis?: number): number

  /**
   * Converts an x value to a percentage from the left to the right of
   * the drawing area.
   *
   * If the coordinate represents a value visible on the canvas, then
   * the value will be between 0 and 1, where 0 is the left of the canvas.
   * However, this method will return values outside the range, as
   * values can fall outside the canvas.
   *
   * If x is null, this returns null.
   * @param {number} x The data x-coordinate.
   * @return {number} A fraction in [0, 1] where 0 = the left edge.
   */
  public toPercentXCoord(x: number): number

  /**
   * Returns the number of columns (including the independent variable).
   */
  public numColumns(): number

  /**
   * Returns the number of rows (excluding any header/label row).
   */
  public numRows(): number

  /**
   * Returns the value in the given row and column. If the row and column exceed
   * the bounds on the data, returns null. Also returns null if the value is
   * missing.
   * @param {number} row The row number of the data (0-based). Row 0 is the
   *         first row of data, not a header row.
   * @param {number} col The column number of the data (0-based)
   * @return {number} The value in the specified cell or null if the row/col
   *         were out of range.
   */
  public getValue(row: number, col: number): number

  /**
   * Detach DOM elements in the dygraph and null out all data references.
   * Calling this when you're done with a dygraph can dramatically reduce memory
   * usage. See, e.g., the tests/perf.html example.
   */
  public destroy(): void

  /**
   * Return the list of colors. This is either the list of colors passed in the
   * attributes or the autogenerated list of rgb(r,g,b) strings.
   * This does not return colors for invisible series.
   * @return {Array.<string>} The list of colors.
   */
  public getColors(): string[]

  /**
   * Returns a few attributes of a series, i.e. its color, its visibility, which
   * axis it's assigned to, and its column in the original data.
   * Returns null if the series does not exist.
   * Otherwise, returns an object with column, visibility, color and axis properties.
   * The "axis" property will be set to 1 for y1 and 2 for y2.
   * The "column" property can be fed back into getValue(row, column) to get
   * values for this series.
   */
  public getPropertiesForSeries(seriesName: string): dygraphs.SeriesProperties

  /**
   * Reset the zoom to the original view coordinates. This is the same as
   * double-clicking on the graph.
   */
  public resetZoom(): void

  /**
   * Get the current graph's area object.
   */
  public getArea(): dygraphs.Area

  /**
   * Convert a mouse event to DOM coordinates relative to the graph origin.
   *
   * Returns a two-element array: [X, Y].
   */
  public eventToDomCoords(event: MouseEvent): [number, number]

  /**
   * Manually set the selected points and display information about them in the
   * legend. The selection can be cleared using clearSelection() and queried
   * using getSelection().
   * @param {number} row Row number that should be highlighted (i.e. appear with
   * hover dots on the chart).
   * @param {seriesName} optional series name to highlight that series with the
   * the highlightSeriesOpts setting.
   * @param { locked } optional If true, keep seriesName selected when mousing
   * over the graph, disabling closest-series highlighting. Call clearSelection()
   * to unlock it.
   */
  public setSelection(
    row: number | boolean,
    seriesName?: string,
    locked?: boolean
  ): void

  /**
   * Clears the current selection (i.e. points that were highlighted by moving
   * the mouse over the chart).
   */
  public clearSelection(): void

  /**
   * Returns the number of the currently selected row. To get data for this row,
   * you can use the getValue method.
   * @return {number} row number, or -1 if nothing is selected
   */
  public getSelection(): number

  /**
   * Returns the name of the currently-highlighted series.
   * Only available when the highlightSeriesOpts option is in use.
   */
  public getHighlightSeries(): string

  /**
   * Returns true if the currently-highlighted series was locked
   * via setSelection(..., seriesName, true).
   */
  public isSeriesLocked(): boolean

  /**
   * Returns the number of y-axes on the chart.
   */
  public numAxes(): number

  /**
   * Changes various properties of the graph. These can include:
   * <ul>
   * <li>file: changes the source data for the graph</li>
   * <li>errorBars: changes whether the data contains stddev</li>
   * </ul>
   *
   * There's a huge variety of options that can be passed to this method. For a
   * full list, see http://dygraphs.com/options.html.
   *
   * @param {Object} input_attrs The new properties and values
   * @param {boolean} block_redraw Usually the chart is redrawn after every
   *         call to updateOptions(). If you know better, you can pass true to
   *         explicitly block the redraw. This can be useful for chaining
   *         updateOptions() calls, avoiding the occasional infinite loop and
   *         preventing redraws when it's not necessary (e.g. when updating a
   *         callback).
   */
  public updateOptions(
    inputAttrs: dygraphs.Options,
    blockRedraw?: boolean
  ): void

  /**
   * Resizes the dygraph. If no parameters are specified, resizes to fill the
   * containing div (which has presumably changed size since the dygraph was
   * instantiated. If the width/height are specified, the div will be resized.
   *
   * This is far more efficient than destroying and re-instantiating a
   * Dygraph, since it doesn't have to reparse the underlying data.
   *
   * @param {number} width Width (in pixels)
   * @param {number} height Height (in pixels)
   */
  public resize(width?: number, height?: number): void

  /**
   * Adjusts the number of points in the rolling average. Updates the graph to
   * reflect the new averaging period.
   * @param {number} length Number of points over which to average the data.
   */
  public adjustRoll(length: number): void

  /**
   * Returns a boolean array of visibility statuses.
   */
  public visibility(): boolean[]

  /**
   * Changes the visiblity of a series.
   *
   * @param {number} num the series index
   * @param {boolean} value true or false, identifying the visibility.
   */
  public setVisibility(num: number, value: boolean): void

  /**
   * Update the list of annotations and redraw the chart.
   * See dygraphs.com/annotations.html for more info on how to use annotations.
   * @param ann {Array} An array of annotation objects.
   * @param suppressDraw {Boolean} Set to "true" to block chart redraw (optional).
   */
  public setAnnotations(
    ann: dygraphs.Annotation[],
    suppressDraw?: boolean
  ): void

  /**
   * Return the list of annotations.
   */
  public annotations(): dygraphs.Annotation[]

  /**
   * Get the list of label names for this graph. The first column is the
   * x-axis, so the data series names start at index 1.
   *
   * Returns null when labels have not yet been defined.
   */
  public getLabels(): string[]

  /**
   * Get the index of a series (column) given its name. The first column is the
   * x-axis, so the data series start with index 1.
   */
  public indexFromSetName(name: string): number

  /**
   * Trigger a callback when the dygraph has drawn itself and is ready to be
   * manipulated. This is primarily useful when dygraphs has to do an XHR for the
   * data (i.e. a URL is passed as the data source) and the chart is drawn
   * asynchronously. If the chart has already drawn, the callback will fire
   * immediately.
   *
   * This is a good place to call setAnnotations().
   */
  public ready(callback: (g: Dygraph) => any): void
}
