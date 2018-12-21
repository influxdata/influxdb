import {BuilderConfig, TimeRange} from 'src/types/v2'
import {FUNCTIONS} from 'src/shared/constants/queryBuilder'

const DEFAULT_WINDOW_INTERVAL = '10s'

const WINDOW_INTERVALS = {
  '5m': '10s',
  '15m': '10s',
  '1h': '1m',
  '6h': '5m',
  '12h': '10m',
  '24h': '20m',
  '2d': '1h',
  '7d': '1h',
  '30d': '6h',
}

export const timeRangeVariables = (
  timeRange: TimeRange
): {[key: string]: string} => {
  const result: {[key: string]: string} = {}

  result.timeRangeStart = timeRange.lower
    .replace('now()', '')
    .replace(/\s/g, '')

  return result
}

export function isConfigValid(builderConfig: BuilderConfig): boolean {
  const {buckets, tags} = builderConfig
  const isConfigValid =
    buckets.length >= 1 &&
    tags.length >= 1 &&
    tags.some(({key, values}) => key && values.length > 0)

  return isConfigValid
}

export function buildQuery(
  builderConfig: BuilderConfig,
  duration: string = '1h'
): string {
  const {functions} = builderConfig

  let query: string

  if (functions.length) {
    query = functions
      .map(f => buildQueryHelper(builderConfig, duration, f))
      .join('\n\n')
  } else {
    query = buildQueryHelper(builderConfig, duration)
  }

  return query
}

function buildQueryHelper(
  builderConfig: BuilderConfig,
  duration: string,
  fn?: BuilderConfig['functions'][0]
): string {
  const [bucket] = builderConfig.buckets
  const tagFilterCall = formatTagFilterCall(builderConfig.tags)
  const fnCall = fn ? formatFunctionCall(fn, duration) : ''

  const query = `from(bucket: "${bucket}")
  |> range(start: -${duration})${tagFilterCall}${fnCall}`

  return query
}

export function formatFunctionCall(
  fn: BuilderConfig['functions'][0],
  duration: string
) {
  const fnSpec = FUNCTIONS.find(f => f.name === fn.name)

  let fnCall: string = ''

  if (fnSpec && fnSpec.aggregate) {
    fnCall = `
  |> window(every: ${WINDOW_INTERVALS[duration] || DEFAULT_WINDOW_INTERVAL})
  ${fnSpec.flux}
  |> group(columns: ["_value", "_time", "_start", "_stop"], mode: "except")
  |> yield(name: "${fn.name}")`
  } else {
    fnCall = `
  ${fnSpec.flux}
  |> yield(name: "${fn.name}")`
  }

  return fnCall
}

export function formatTagFilterCall(tagsSelections: BuilderConfig['tags']) {
  if (!tagsSelections.length) {
    return ''
  }

  const calls = tagsSelections
    .filter(({key, values}) => key && values.length)
    .map(({key, values}) => {
      const fnBody = values.map(value => `r.${key} == "${value}"`).join(' or ')

      return `|> filter(fn: (r) => ${fnBody})`
    })
    .join('\n  ')

  return `\n  ${calls}`
}
