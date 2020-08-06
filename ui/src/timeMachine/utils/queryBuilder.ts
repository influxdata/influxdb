import {get, isEmpty} from 'lodash'
import {
  BuilderConfig,
  BuilderTagsType,
  DashboardDraftQuery,
  CheckType,
  Threshold,
} from 'src/types'
import {FUNCTIONS} from 'src/timeMachine/constants/queryBuilder'
import {
  TIME_RANGE_START,
  TIME_RANGE_STOP,
  OPTION_NAME,
  WINDOW_PERIOD,
} from 'src/variables/constants'
import {AGG_WINDOW_AUTO} from 'src/timeMachine/constants/queryBuilder'

export function isConfigValid(builderConfig: BuilderConfig): boolean {
  const {buckets, tags} = builderConfig

  const isConfigValid =
    buckets.length >= 1 &&
    tags.length >= 1 &&
    tags.some(({key, values}) => key && values.length > 0)

  return isConfigValid
}

export const isConfigEmpty = (builderConfig: BuilderConfig): boolean => {
  const {buckets, tags} = builderConfig
  const isConfigEmpty =
    buckets.length <= 1 &&
    !tags.some(({key, values}) => key && values.length > 0)

  return isConfigEmpty
}

export interface CheckQueryValidity {
  oneQuery: boolean
  builderMode: boolean
  singleAggregateFunc: boolean
  singleField: boolean
}

export const isDraftQueryAlertable = (
  draftQueries: DashboardDraftQuery[]
): CheckQueryValidity => {
  const tags: BuilderTagsType[] = get(
    draftQueries,
    '[0].builderConfig.tags',
    []
  )
  const fieldSelection = tags.find(t => get(t, 'key') === '_field')
  const fieldValues = get(fieldSelection, 'values', [])
  const functions = draftQueries[0].builderConfig.functions
  return {
    oneQuery: draftQueries.length === 1, // one query
    builderMode: draftQueries[0].editMode == 'builder', // built in builder
    singleAggregateFunc: functions.length === 1, // one aggregate function
    singleField: fieldValues.length === 1, // one field selection
  }
}

export const isCheckSaveable = (
  draftQueries: DashboardDraftQuery[],
  checkType: CheckType,
  thresholds: Threshold[]
): boolean => {
  const {
    oneQuery,
    builderMode,
    singleAggregateFunc,
    singleField,
  } = isDraftQueryAlertable(draftQueries)

  if (checkType === 'custom') {
    return true
  }

  if (checkType === 'deadman') {
    return oneQuery && builderMode && singleField
  }

  return (
    oneQuery &&
    builderMode &&
    singleAggregateFunc &&
    singleField &&
    !!thresholds.length
  )
}

export function buildQuery(builderConfig: BuilderConfig): string {
  const {functions} = builderConfig

  let query: string
  if (functions.length) {
    query = functions
      .map(f => buildQueryFromConfig(builderConfig, f))
      .join('\n\n')
  } else {
    query = buildQueryFromConfig(builderConfig, null)
  }

  return query
}

function buildQueryFromConfig(
  builderConfig: BuilderConfig,
  fn?: BuilderConfig['functions'][0]
): string {
  const [bucket] = builderConfig.buckets

  const tags = Array.from(builderConfig.tags)

  // todo: (bucky) - check to see if we can combine filter calls
  // https://github.com/influxdata/influxdb/issues/16076
  let tagsFunctionCalls = ''
  tags.forEach(tag => {
    tagsFunctionCalls += convertTagsToFluxFunctionString(tag)
  })

  const {aggregateWindow} = builderConfig
  const fnCall = fn
    ? formatFunctionCall(fn, aggregateWindow.period, aggregateWindow.fillValues)
    : ''

  const query = `from(bucket: "${bucket}")
  |> range(start: ${OPTION_NAME}.${TIME_RANGE_START}, stop: ${OPTION_NAME}.${TIME_RANGE_STOP})${tagsFunctionCalls}${fnCall}`

  return query
}

export function formatFunctionCall(
  fn: BuilderConfig['functions'][0],
  period: string,
  fillValues: boolean
) {
  const fnSpec = FUNCTIONS.find(spec => spec.name === fn.name)

  if (!fnSpec) {
    return
  }

  const formattedPeriod = formatPeriod(period)

  return `\n  ${fnSpec.flux(formattedPeriod, fillValues)}\n  |> yield(name: "${
    fn.name
  }")`
}

const convertTagsToFluxFunctionString = function convertTagsToFluxFunctionString(
  tag: BuilderTagsType
) {
  if (!tag.key) {
    return ''
  }

  if (tag.aggregateFunctionType === 'filter') {
    if (!tag.values.length) {
      return ''
    }

    return `\n  |> filter(fn: (r) => ${tagToFlux(tag)})`
  }

  if (tag.aggregateFunctionType === 'group') {
    const quotedValues = tag.values.map(value => `"${value}"`) // wrap the value in double quotes

    if (quotedValues.length) {
      return `\n  |> group(columns: [${quotedValues.join(', ')}])` // join with a comma (e.g. "foo","bar","baz")
    }

    return '\n  |> group()'
  }

  return ''
}

export const tagToFlux = function tagToFlux(tag: BuilderTagsType) {
  return tag.values
    .map(value => `r["${tag.key}"] == "${value.replace(/\\/g, '\\\\')}"`)
    .join(' or ')
}

const formatPeriod = (period: string): string => {
  if (period === AGG_WINDOW_AUTO || !period) {
    return `${OPTION_NAME}.${WINDOW_PERIOD}`
  }

  return period
}

export enum ConfirmationState {
  NotRequired = 'no confirmation required',
  Required = 'confirmation required',
  Unknown = 'unknown confirmation state',
}

export const confirmationState = (
  query: string,
  builderConfig: BuilderConfig
) => {
  if (
    !isConfigValid(builderConfig) ||
    !hasQueryBeenEdited(query, builderConfig)
  ) {
    ConfirmationState.NotRequired
  }

  if (hasQueryBeenEdited(query, builderConfig) || isEmpty(query)) {
    return ConfirmationState.Required
  }

  return ConfirmationState.NotRequired
}

export function hasQueryBeenEdited(
  query: string,
  builderConfig: BuilderConfig
): boolean {
  const _isQueryDifferent = query !== buildQuery(builderConfig)

  return _isQueryDifferent
}
