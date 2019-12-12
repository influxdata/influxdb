import {get, isEmpty} from 'lodash'
import {BuilderConfig, DashboardDraftQuery, Check} from 'src/types'
import {FUNCTIONS} from 'src/timeMachine/constants/queryBuilder'
import {
  TIME_RANGE_START,
  TIME_RANGE_STOP,
  OPTION_NAME,
  WINDOW_PERIOD,
} from 'src/variables/constants'
import {AGG_WINDOW_AUTO} from 'src/timeMachine/constants/queryBuilder'
import {BuilderTagsType} from '@influxdata/influx'

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
  check: Partial<Check>
): boolean => {
  const {
    oneQuery,
    builderMode,
    singleAggregateFunc,
    singleField,
  } = isDraftQueryAlertable(draftQueries)

  if (check.type === 'deadman') {
    return oneQuery && builderMode && singleField
  }

  return (
    oneQuery &&
    builderMode &&
    singleAggregateFunc &&
    singleField &&
    check.thresholds &&
    !!check.thresholds.length
  )
}

export function buildQuery(builderConfig: BuilderConfig): string {
  const {functions} = builderConfig

  let query: string

  if (functions.length) {
    query = functions.map(f => buildQueryHelper(builderConfig, f)).join('\n\n')
  } else {
    query = buildQueryHelper(builderConfig)
  }

  return query
}

function buildQueryHelper(
  builderConfig: BuilderConfig,
  fn?: BuilderConfig['functions'][0]
): string {
  const [bucket] = builderConfig.buckets
  const tagFilterCall = formatTagFilterCall(builderConfig.tags)
  const {aggregateWindow} = builderConfig
  const fnCall = fn ? formatFunctionCall(fn, aggregateWindow.period) : ''

  const query = `from(bucket: "${bucket}")
  |> range(start: ${OPTION_NAME}.${TIME_RANGE_START}, stop: ${OPTION_NAME}.${TIME_RANGE_STOP})${tagFilterCall}${fnCall}`

  return query
}

export function formatFunctionCall(
  fn: BuilderConfig['functions'][0],
  period: string
) {
  const fnSpec = FUNCTIONS.find(spec => spec.name === fn.name)

  if (!fnSpec) {
    return
  }

  const formattedPeriod = formatPeriod(period)

  return `\n  ${fnSpec.flux(formattedPeriod)}\n  |> yield(name: "${fn.name}")`
}

const formatPeriod = (period: string): string => {
  if (period === AGG_WINDOW_AUTO || !period) {
    return `${OPTION_NAME}.${WINDOW_PERIOD}`
  }

  return period
}

function formatTagFilterCall(tagsSelections: BuilderConfig['tags']) {
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

export function createCheckQueryFromParams(
  builderConfig: BuilderConfig,
  check: Partial<Check>
): string {
  const dataFrom = `data = from(bucket: \"${builderConfig.buckets[0]}\")`

  const filterStatements = builderConfig.tags
    .filter(tag => !!tag.values[0])
    .map(tag => `  |> filter(fn: (r) => r.${tag.key} == \"${tag.values[0]}\")`)

  const messageFn = `messageFn = (r) =>(\"${check.statusMessageTemplate}\")`

  const checkTags = check.tags
    ? check.tags
        .filter(t => t.key && t.value)
        .map(t => `${t.key}: \"${t.value}\"`)
        .join(',')
    : ''

  const checkStatement = [
    'check = {',
    `  _check_id: \"${check.id}\",`, //PROBLEM: WHAT IF CHECK DOES NOT EXIST YET.
    `  _check_name: \"${check.name}\",`,
    `  _type: \"${check.type}\",`,
    `  tags: {${checkTags}}`,
    '}',
  ]

  const optionTask = [
    'option task = {',
    `  name: \"${check.name}\",`,
    `  every: ${check.every},`,
    `  offset: ${check.offset}`,
    '}',
  ]

  if (check.type === 'deadman') {
    const imports = [
      'package main',
      'import "influxdata/influxdb/monitor"',
      'import "experimental"',
      'import "influxdata/influxdb/v1"',
    ]

    const dataRange = `  |> range(start: -${check.staleTime})`

    //insert variable here.

    const dataDefinition = [dataFrom, dataRange, ...filterStatements]

    const levelFunction = `${check.level.toLowerCase()} = (r) => (r.dead)`

    const checkLevel = `${check.level.toLowerCase()}:${check.level.toLowerCase()}`

    const queryStatement = [
      'data',
      '  |> v1.fieldsAsCols()',
      `  |> monitor.deadman(t: experimental.subDuration(from: now(), d: ${
        check.timeSince
      }))`,
      `  |> monitor.check(data: check, messageFn: messageFn,${checkLevel})`,
    ]

    const script: string[] = [
      imports.join('\n'),
      dataDefinition.join('\n'),
      optionTask.join('\n'),
      checkStatement.join('\n'),
      levelFunction,
      messageFn,
      queryStatement.join('\n'),
    ]
    return script.join('\n\n')
  }
  if (check.type === 'threshold') {
    const imports = [
      'package main',
      'import "influxdata/influxdb/monitor"',
      'import "influxdata/influxdb/v1"',
    ]

    const dataRange = `  |> range(start: -${check.every})`

    const aggregateFunction = `  |> aggregateWindow(every: ${check.every} fn: ${
      builderConfig.functions[0].name
    }, createEmpty: false)`

    const dataDefinition = [
      dataFrom,
      dataRange,
      ...filterStatements,
      aggregateFunction,
    ]

    const thresholds = check.thresholds.map(t => {
      const fieldTag = builderConfig.tags.find(t => t.key === '_field')
      const fieldSelection = get(fieldTag, 'values.[0]')

      // "crit = (r) =>(r.fieldName"
      const beginning = `${t.level.toLowerCase()} = (r) =>(r.${fieldSelection}`

      if (t.type === 'range') {
        if (t.within) {
          return `${beginning} > ${t.min}) and r.${fieldSelection} < ${t.max})`
        } else {
          return `${beginning} < ${t.min} and r.${fieldSelection} > ${t.max})`
        }
      } else {
        const equality = t.type === 'greater' ? '>' : '<'

        return `${beginning}${equality} ${t.value})`
      }
    })

    const thresholdsDefined = check.thresholds.map(
      t => ` ${t.level.toLowerCase()}:${t.level.toLowerCase()}`
    )

    const queryStatement = [
      'data',
      '  |> v1.fieldsAsCols()',
      `  |> monitor.check(data: check, messageFn: messageFn,${thresholdsDefined})`,
    ]

    const script: string[] = [
      imports.join('\n'),
      dataDefinition.join('\n'),
      optionTask.join('\n'),
      checkStatement.join('\n'),
      thresholds.join('\n'),
      messageFn,
      queryStatement.join('\n'),
    ]
    return script.join('\n\n')
  }
}
