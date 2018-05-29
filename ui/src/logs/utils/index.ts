import uuid from 'uuid'
import {TimeRange, Namespace, QueryConfig} from 'src/types'

const fields = [
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

const defaultQueryConfig = {
  areTagsAccepted: false,
  fields,
  fill: '0',
  groupBy: {time: '2m', tags: []},
  measurement: 'syslog',
  rawText: null,
  shifts: [],
  tags: {},
}

export const buildHistogramQueryConfig = (
  namespace: Namespace,
  range: TimeRange
): QueryConfig => {
  const id = uuid.v4()
  return {
    ...defaultQueryConfig,
    id,
    range,
    database: namespace.database,
    retentionPolicy: namespace.retentionPolicy,
  }
}
