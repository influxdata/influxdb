import {extent, ticks} from 'd3-array'
import {scaleLinear, scaleOrdinal} from 'd3-scale'
import {produce} from 'immer'
import chroma from 'chroma-js'

import {
  PlotEnv,
  Layer,
  HistogramLayer,
  Scale,
  PLOT_PADDING,
  TICK_CHAR_WIDTH,
  TICK_CHAR_HEIGHT,
  TICK_PADDING_RIGHT,
  TICK_PADDING_TOP,
} from 'src/minard'
import {PlotAction} from 'src/minard/utils/plotEnvActions'
import {getGroupKey} from 'src/minard/utils/getGroupKey'

const DEFAULT_X_DOMAIN: [number, number] = [0, 1]
const DEFAULT_Y_DOMAIN: [number, number] = [0, 1]

export const INITIAL_PLOT_ENV: PlotEnv = {
  width: 0,
  height: 0,
  innerWidth: 0,
  innerHeight: 0,
  margins: {
    top: PLOT_PADDING,
    right: PLOT_PADDING,
    bottom: PLOT_PADDING,
    left: PLOT_PADDING,
  },
  xTicks: [],
  yTicks: [],
  xDomain: null,
  yDomain: null,
  baseLayer: {
    type: 'base',
    table: {columns: {}, length: 0},
    xDomain: DEFAULT_X_DOMAIN,
    yDomain: DEFAULT_Y_DOMAIN,
    mappings: {},
    scales: {
      x: null,
      y: null,
    },
  },
  layers: {},
  hoverX: null,
  hoverY: null,
  dispatch: () => {},
}

export const plotEnvReducer = (state: PlotEnv, action: PlotAction): PlotEnv =>
  produce(state, draftState => {
    switch (action.type) {
      case 'REGISTER_LAYER': {
        const {layerKey, layer} = action.payload

        draftState.layers[layerKey] = {...layer, scales: {}} as Layer

        setXDomain(draftState)
        setYDomain(draftState)
        setLayout(draftState)
        setFillScales(draftState)

        return
      }

      case 'UNREGISTER_LAYER': {
        const {layerKey} = action.payload

        delete draftState.layers[layerKey]

        setXDomain(draftState)
        setYDomain(draftState)
        setLayout(draftState)
        setFillScales(draftState)

        return
      }

      case 'SET_DIMENSIONS': {
        const {width, height} = action.payload

        draftState.width = width
        draftState.height = height

        setLayout(draftState)

        return
      }

      case 'SET_TABLE': {
        draftState.baseLayer.table = action.payload.table

        return
      }

      case 'SET_CONTROLLED_X_DOMAIN': {
        const {xDomain} = action.payload

        draftState.xDomain = xDomain

        setXDomain(draftState)
        setLayout(draftState)

        return
      }

      case 'SET_CONTROLLED_Y_DOMAIN': {
        const {yDomain} = action.payload

        draftState.yDomain = yDomain

        setYDomain(draftState)
        setLayout(draftState)

        return
      }
    }
  })

/*
  Find all columns in the current in all layers that are mapped to the supplied
  aesthetic mappings
*/
const getColumnsForAesthetics = (
  state: PlotEnv,
  mappings: string[]
): any[][] => {
  const {baseLayer, layers} = state

  const cols = []

  for (const layer of Object.values(layers)) {
    for (const mapping of mappings) {
      const colName = layer.mappings[mapping]

      if (colName) {
        const col = layer.table
          ? layer.table.columns[colName].data
          : baseLayer.table.columns[colName].data

        cols.push(col)
      }
    }
  }

  return cols
}

/*
  Flatten an array of arrays by one level
*/
const flatten = (arrays: any[][]): any[] => [].concat(...arrays)

/*
  Given a list of aesthetics, find the domain across all columns in all layers
  that are mapped to that aesthetic
*/
const getDomainForAesthetics = (
  state: PlotEnv,
  aesthetics: string[]
): [number, number] => {
  const domains = getColumnsForAesthetics(state, aesthetics).map(col =>
    extent(col)
  )

  const domainOfDomains = extent(flatten(domains))

  if (domainOfDomains.some(x => x === undefined)) {
    return null
  }

  return domainOfDomains
}

/*
  If the x domain is in "controlled" mode, set it according to the passed
  `xDomain` prop. Otherwise compute and set the domain based on the extent of
  relevant data in each layer.
*/
const setXDomain = (draftState: PlotEnv): void => {
  if (draftState.xDomain) {
    draftState.baseLayer.xDomain = draftState.xDomain
  } else {
    draftState.baseLayer.xDomain =
      getDomainForAesthetics(draftState, ['x', 'xMin', 'xMax']) ||
      DEFAULT_X_DOMAIN
  }
}

/*
  See `setXDomain`.
*/
const setYDomain = (draftState: PlotEnv): void => {
  if (draftState.yDomain) {
    draftState.baseLayer.yDomain = draftState.yDomain
  } else {
    draftState.baseLayer.yDomain =
      getDomainForAesthetics(draftState, ['y', 'yMin', 'yMax']) ||
      DEFAULT_Y_DOMAIN
  }
}

const getTicks = ([d0, d1]: number[], length: number): number[] => {
  const approxTickWidth =
    Math.max(String(d0).length, String(d1).length) * TICK_CHAR_WIDTH

  const TICK_DENSITY = 0.3
  const numTicks = Math.round((length / approxTickWidth) * TICK_DENSITY)
  const result = ticks(d0, d1, numTicks)

  return result
}

/*
  Compute and set the ticks, margins, x/y scales, and dimensions for the plot.
*/
const setLayout = (draftState: PlotEnv): void => {
  const {width, height} = draftState
  const xDomain = draftState.xDomain || draftState.baseLayer.xDomain
  const yDomain = draftState.yDomain || draftState.baseLayer.yDomain

  draftState.xTicks = getTicks(xDomain, width)
  draftState.yTicks = getTicks(yDomain, height)

  const yTickWidth =
    Math.max(...draftState.yTicks.map(t => String(t).length)) * TICK_CHAR_WIDTH

  const margins = {
    top: PLOT_PADDING,
    right: PLOT_PADDING,
    bottom: TICK_CHAR_HEIGHT + TICK_PADDING_TOP + PLOT_PADDING,
    left: yTickWidth + TICK_PADDING_RIGHT + PLOT_PADDING,
  }

  const innerWidth = width - margins.left - margins.right
  const innerHeight = height - margins.top - margins.bottom

  draftState.margins = margins
  draftState.innerWidth = innerWidth
  draftState.innerHeight = innerHeight

  draftState.baseLayer.scales.x = scaleLinear()
    .domain(xDomain)
    .range([0, innerWidth])

  draftState.baseLayer.scales.y = scaleLinear()
    .domain(yDomain)
    .range([innerHeight, 0])
}

/*
  Get a scale that maps elements of the domain to a color according to the
  color scheme passed as `colors`.
*/
const getColorScale = (
  domain: string[],
  colors: string[]
): Scale<string, string> => {
  const range = chroma
    .scale(colors)
    .mode('lch')
    .colors(domain.length)

  const scale = scaleOrdinal<string>()
    .domain(domain)
    .range(range)

  return scale
}

/*
  Get the domain for the scale used for the data-to-fill aesthetic mapping.

  The fill aesthetic is always used to visually distinguish different groupings
  of data (for now). So the domain of the scale is a set of "group keys" which
  represent all possible groupings of data in the layer.
*/
const getFillDomain = ({table, mappings}: HistogramLayer): string[] => {
  const fillColKeys = mappings.fill

  if (!fillColKeys.length) {
    return []
  }

  const fillDomain = new Set()

  for (let i = 0; i < table.length; i++) {
    fillDomain.add(getGroupKey(fillColKeys.map(k => table.columns[k].data[i])))
  }

  return [...fillDomain].sort()
}

/*
  For each layer, compute and set a fill scale according to the layer's
  data-to-fill mapping.
*/
const setFillScales = (draftState: PlotEnv) => {
  const layers = Object.values(draftState.layers)

  layers
    .filter(layer => layer.type === 'histogram')
    .forEach((layer: HistogramLayer) => {
      layer.scales.fill = getColorScale(getFillDomain(layer), layer.colors)
    })
}
