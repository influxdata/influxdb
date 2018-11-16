import _ from 'lodash'
import moment from 'moment'
import {Filter, LogSearchParams} from 'src/types/logs'
import {TimeRange, QueryConfig} from 'src/types'
import {NULL_STRING} from 'src/shared/constants/queryFillOptions'

import {
  quoteIfTimestamp,
  buildSelect,
  buildWhereClause,
  buildGroupBy,
  buildFill,
} from 'src/utils/influxql'

import {DEFAULT_TIME_FORMAT} from 'src/logs/constants'

const keyMapping = (key: string): string => {
  switch (key) {
    case 'severity_1':
      return 'severity'
    default:
      return key
  }
}

const operatorMapping = (operator: string): string => {
  switch (operator) {
    case '==':
      return '='
    default:
      return operator
  }
}

const valueMapping = (operator: string, value): string => {
  if (operator === '=~' || operator === '!~') {
    return `${new RegExp(value)}`
  } else {
    return `'${value}'`
  }
}

export const filtersClause = (filters: Filter[]): string => {
  return _.map(
    filters,
    (filter: Filter) =>
      `"${keyMapping(filter.key)}" ${operatorMapping(
        filter.operator
      )} ${valueMapping(filter.operator, filter.value)}`
  ).join(' AND ')
}

export function buildInfiniteScrollWhereClause({
  lower,
  upper,
  tags,
  areTagsAccepted,
}: QueryConfig): string {
  const timeClauses = []

  if (lower) {
    timeClauses.push(`time >= '${lower}'`)
  }

  if (upper) {
    timeClauses.push(`time < '${upper}'`)
  }

  const tagClauses = _.keys(tags).map(k => {
    const operator = areTagsAccepted ? '=' : '!='

    if (tags[k].length > 1) {
      const joinedOnOr = tags[k]
        .map(v => `"${k}"${operator}'${v}'`)
        .join(' OR ')
      return `(${joinedOnOr})`
    }

    return `"${k}"${operator}'${tags[k]}'`
  })

  const subClauses = timeClauses.concat(tagClauses)
  if (!subClauses.length) {
    return ''
  }

  return ` WHERE ${subClauses.join(' AND ')}`
}

export function buildGeneralLogQuery(
  condition: string,
  config: QueryConfig,
  filters: Filter[]
) {
  const {groupBy, fill = NULL_STRING} = config
  const select = buildSelect(config, '')
  const dimensions = buildGroupBy(groupBy)
  const fillClause = groupBy.time ? buildFill(fill) : ''

  if (!_.isEmpty(filters)) {
    condition = `${condition} AND ${filtersClause(filters)}`
  }

  return `${select}${condition}${dimensions}${fillClause}`
}

export function buildInfluxQLQuery({
  lower,
  upper,
  config,
  filters,
}: LogSearchParams) {
  const {tags, areTagsAccepted} = config

  const condition = buildInfiniteScrollWhereClause({
    lower,
    upper,
    tags,
    areTagsAccepted,
  })

  return buildGeneralLogQuery(condition, config, filters)
}

export function buildLogQuery(
  timeRange: TimeRange,
  config: QueryConfig,
  filters: Filter[],
  searchTerm: string | null = null
): string {
  const {groupBy, fill = NULL_STRING, tags, areTagsAccepted} = config
  const {upper, lower} = quoteIfTimestamp(timeRange)
  const select = buildSelect(config, '')
  const dimensions = buildGroupBy(groupBy)
  const fillClause = groupBy.time ? buildFill(fill) : ''

  let condition = buildWhereClause({lower, upper, tags, areTagsAccepted})
  if (!_.isEmpty(searchTerm)) {
    condition = `${condition} AND message =~ ${new RegExp(searchTerm)}`
  }

  if (!_.isEmpty(filters)) {
    condition = `${condition} AND ${filtersClause(filters)}`
  }

  return `${select}${condition}${dimensions}${fillClause}`
}

export const formatTime = (time: number): string => {
  return moment(time).format(DEFAULT_TIME_FORMAT)
}
