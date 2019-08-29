export const AGG_WINDOW_AUTO = 'auto'
export const AGG_WINDOW_NONE = 'none'

export const DURATIONS = [
  {duration: '5s', displayText: 'Every 5 seconds'},
  {duration: '15s', displayText: 'Every 15 seconds'},
  {duration: '1m', displayText: 'Every minute'},
  {duration: '5m', displayText: 'Every 5 minutes'},
  {duration: '15m', displayText: 'Every 15 minutes'},
  {duration: '1h', displayText: 'Every hour'},
  {duration: '6h', displayText: 'Every 6 hours'},
  {duration: '12h', displayText: 'Every 12 hours'},
  {duration: '24h', displayText: 'Every 24 hours'},
  {duration: '2d', displayText: 'Every 2 days'},
  {duration: '7d', displayText: 'Every 7 days'},
  {duration: '30d', displayText: 'Every 30 days'},
]

export const AUTO_NONE_DURATIONS = [
  {duration: AGG_WINDOW_AUTO, displayText: 'Auto'},
  {duration: AGG_WINDOW_NONE, displayText: 'None'},
]

export interface QueryFn {
  name: string
  flux: (period?: string) => string
}

export const genFlux = (func: string, period?: string) => {
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
      return `|> aggregateWindow(every: ${period}, fn: ${func})`
    }

    default:
      return `|> ${func}()`
  }
}

export const FUNCTIONS: QueryFn[] = [
  {
    name: 'mean',
    flux: period => genFlux('mean', period),
  },
  {
    name: 'median',
    flux: period => genFlux('median', period),
  },
  {
    name: 'max',
    flux: period => genFlux('max', period),
  },
  {
    name: 'min',
    flux: period => genFlux('min', period),
  },
  {
    name: 'sum',
    flux: period => genFlux('sum', period),
  },
  {
    name: 'derivative',
    flux: period => genFlux('derivative', period),
  },
  {
    name: 'nonnegative derivative',
    flux: period => genFlux('nonnegative derivative', period),
  },
  {
    name: 'distinct',
    flux: period => genFlux('distinct', period),
  },
  {
    name: 'count',
    flux: period => genFlux('count', period),
  },
  {
    name: 'increase',
    flux: period => genFlux('increase', period),
  },
  {
    name: 'skew',
    flux: period => genFlux('skew', period),
  },
  {
    name: 'spread',
    flux: period => genFlux('spread', period),
  },
  {
    name: 'stddev',
    flux: period => genFlux('stddev', period),
  },
  {
    name: 'first',
    flux: period => genFlux('first', period),
  },
  {
    name: 'last',
    flux: period => genFlux('last', period),
  },
  {
    name: 'unique',
    flux: period => genFlux('unique', period),
  },
  {
    name: 'sort',
    flux: period => genFlux('sort', period),
  },
]
