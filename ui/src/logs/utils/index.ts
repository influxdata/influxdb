import _ from 'lodash'
import moment from 'moment'
import uuid from 'uuid'
import {Filter} from 'src/types/logs'
import {TimeRange, Namespace, QueryConfig} from 'src/types'
import {NULL_STRING} from 'src/shared/constants/queryFillOptions'
import {getDeep} from 'src/utils/wrappers'
import {
  quoteIfTimestamp,
  buildSelect,
  buildWhereClause,
  buildGroupBy,
  buildFill,
} from 'src/utils/influxql'

import {HistogramData} from 'src/types/histogram'

const BIN_COUNT = 30

const histogramFields = [
  {
    alias: '',
    args: [
      {
        alias: 'message',
        type: 'field',
        value: 'message',
      },
    ],
    type: 'func',
    value: 'count',
  },
]

const tableFields = [
  {
    alias: 'severity',
    type: 'field',
    value: 'severity',
  },
  {
    alias: 'timestamp',
    type: 'field',
    value: 'timestamp',
  },
  {
    alias: 'message',
    type: 'field',
    value: 'message',
  },
  {
    alias: 'facility',
    type: 'field',
    value: 'facility',
  },
  {
    alias: 'procid',
    type: 'field',
    value: 'procid',
  },
  {
    alias: 'appname',
    type: 'field',
    value: 'appname',
  },
  {
    alias: 'host',
    type: 'field',
    value: 'host',
  },
]

const defaultQueryConfig = {
  areTagsAccepted: false,
  fill: '0',
  measurement: 'syslog',
  rawText: null,
  shifts: [],
  tags: {},
}

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
  if (operator === '=~') {
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

const computeSeconds = (range: TimeRange) => {
  const {upper, lower, seconds} = range

  if (seconds) {
    return seconds
  } else if (upper && upper.match(/now/) && lower) {
    return moment().unix() - moment(lower).unix()
  } else if (upper && lower) {
    return moment(upper).unix() - moment(lower).unix()
  } else {
    return 120
  }
}

const createGroupBy = (range: TimeRange) => {
  const seconds = computeSeconds(range)
  const time = `${Math.max(Math.floor(seconds / BIN_COUNT), 1)}s`
  const tags = ['severity']

  return {time, tags}
}

export const buildHistogramQueryConfig = (
  namespace: Namespace,
  range: TimeRange
): QueryConfig => {
  const id = uuid.v4()
  const {database, retentionPolicy} = namespace

  return {
    ...defaultQueryConfig,
    id,
    range,
    database,
    retentionPolicy,
    groupBy: createGroupBy(range),
    fields: histogramFields,
  }
}

export const buildTableQueryConfig = (
  namespace: Namespace,
  range: TimeRange
): QueryConfig => {
  const id = uuid.v4()
  const {database, retentionPolicy} = namespace

  return {
    ...defaultQueryConfig,
    id,
    range,
    database,
    retentionPolicy,
    groupBy: {tags: []},
    fields: tableFields,
    fill: null,
  }
}

export const parseHistogramQueryResponse = (
  response: object
): HistogramData => {
  const series = getDeep<any[]>(response, 'results.0.series', [])
  const data = series.reduce((acc, current) => {
    const group = getDeep<string>(current, 'tags.severity', '')

    if (!current.columns || !current.values) {
      return acc
    }

    const timeColIndex = current.columns.findIndex(v => v === 'time')
    const countColIndex = current.columns.findIndex(v => v === 'count')

    if (timeColIndex < 0 || countColIndex < 0) {
      return acc
    }

    const vs = current.values.map(v => {
      const time = v[timeColIndex]
      const value = v[countColIndex]

      return {
        key: `${group}-${value}-${time}`,
        time,
        value,
        group,
      }
    })

    return [...acc, ...vs]
  }, [])

  return data
}
