export const AGG_WINDOW_AUTO = 'auto'
export const AGG_WINDOW_NONE = 'none'

export const DURATIONS = [
  '5s',
  '15s',
  '1m',
  '5m',
  '15m',
  '1h',
  '6h',
  '12h',
  '24h',
  '2d',
  '7d',
  '30d',
]

export interface QueryFn {
  name: string
  flux: (period?: string, fillValues?: boolean) => string
}

export const genFlux = (
  func: string,
  period?: string,
  fillValues?: boolean
) => {
  if (period === AGG_WINDOW_NONE) {
    return `|> ${func}()`
  }

  switch (func) {
    case 'derivative': {
      return `|> derivative(unit: ${period}, nonNegative: false)`
    }

    case 'nonnegative derivative': {
      return `|> derivative(unit: ${period}, nonNegative: true)`
    }

    case 'median':
    case 'mean':
    case 'max':
    case 'min':
    case 'sum':
    case 'stddev':
    case 'first':
    case 'last': {
      return `|> aggregateWindow(every: ${period}, fn: ${func}, createEmpty: ${!fillValues})`
    }

    default:
      return `|> ${func}()`
  }
}

export const AUTO_FUNCTIONS: QueryFn[] = [
  {
    name: 'mean',
    flux: (period, fillValues) => genFlux('mean', period, fillValues),
  },
  {
    name: 'median',
    flux: (period, fillValues) => genFlux('median', period, fillValues),
  },
  {
    name: 'first',
    flux: (period, fillValues) => genFlux('first', period, fillValues),
  },
]

export const FUNCTIONS: QueryFn[] = [
  AUTO_FUNCTIONS[0],
  AUTO_FUNCTIONS[1],
  {
    name: 'max',
    flux: (period, fillValues) => genFlux('max', period, fillValues),
  },
  {
    name: 'min',
    flux: (period, fillValues) => genFlux('min', period, fillValues),
  },
  {
    name: 'sum',
    flux: (period, fillValues) => genFlux('sum', period, fillValues),
  },
  {
    name: 'derivative',
    flux: (period, fillValues) => genFlux('derivative', period, fillValues),
  },
  {
    name: 'nonnegative derivative',
    flux: (period, fillValues) =>
      genFlux('nonnegative derivative', period, fillValues),
  },
  {
    name: 'distinct',
    flux: (period, fillValues) => genFlux('distinct', period, fillValues),
  },
  {
    name: 'count',
    flux: (period, fillValues) => genFlux('count', period, fillValues),
  },
  {
    name: 'increase',
    flux: (period, fillValues) => genFlux('increase', period, fillValues),
  },
  {
    name: 'skew',
    flux: (period, fillValues) => genFlux('skew', period, fillValues),
  },
  {
    name: 'spread',
    flux: (period, fillValues) => genFlux('spread', period, fillValues),
  },
  {
    name: 'stddev',
    flux: (period, fillValues) => genFlux('stddev', period, fillValues),
  },
  AUTO_FUNCTIONS[2],
  {
    name: 'last',
    flux: (period, fillValues) => genFlux('last', period, fillValues),
  },
  {
    name: 'unique',
    flux: (period, fillValues) => genFlux('unique', period, fillValues),
  },
  {
    name: 'sort',
    flux: (period, fillValues) => genFlux('sort', period, fillValues),
  },
]
