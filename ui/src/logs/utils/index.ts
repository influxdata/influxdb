import _ from 'lodash'
import moment from 'moment'
import uuid from 'uuid'
import {TimeRange, Namespace, QueryConfig} from 'src/types'
import {NULL_STRING} from 'src/shared/constants/queryFillOptions'
import {
  quoteIfTimestamp,
  buildSelect,
  buildWhereClause,
  buildGroupBy,
  buildFill,
} from 'src/utils/influxql'

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
    alias: 'severity_text',
    type: 'field',
    value: 'severity',
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
  {
    alias: 'message',
    type: 'field',
    value: 'message',
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

export function buildLogQuery(
  timeRange: TimeRange,
  config: QueryConfig,
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

  return `${select}${condition}${dimensions}${fillClause}`
}

const computeSeconds = (range: TimeRange) => {
  const {upper, lower, seconds} = range

  if (seconds) {
    return seconds
  } else if (upper && lower) {
    return moment(upper).unix() - moment(lower).unix()
  } else {
    return 120
  }
}

const createGroupBy = (range: TimeRange) => {
  const seconds = computeSeconds(range)
  const time = `${Math.floor(seconds / BIN_COUNT)}s`
  const tags = []

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
