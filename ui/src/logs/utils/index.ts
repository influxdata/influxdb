import moment from 'moment'
import uuid from 'uuid'
import {TimeRange, Namespace, QueryConfig} from 'src/types'

const BIN_COUNT = 30

const histogramFields = [
  {
    alias: '',
    args: [
      {
        alias: '',
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
    alias: 'timestamp',
    type: 'field',
    value: 'timestamp',
  },
  {
    alias: 'facility_code',
    type: 'field',
    value: 'facility_code',
  },
  {
    alias: 'procid',
    type: 'field',
    value: 'procid',
  },
  {
    alias: 'severity_code',
    type: 'field',
    value: 'severity_code',
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
