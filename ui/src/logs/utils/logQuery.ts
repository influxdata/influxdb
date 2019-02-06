// Libraries
import uuid from 'uuid'
import _ from 'lodash'

// Utils
import {buildFluxQuery} from 'src/logs/utils/v2/queryBuilder'
import {buildInfluxQLQuery} from 'src/logs/utils/v1/queryBuilder'

// Types
import {Bucket, Query} from 'src/api'
import {QueryConfig} from 'src/types'
import {LogSearchParams, LogQuery} from 'src/types/logs'

const defaultQueryConfig = {
  areTagsAccepted: false,
  fill: '0',
  measurement: 'syslog',
  rawText: null,
  shifts: [],
  tags: {},
}

const tableFields = [
  {
    alias: 'time',
    type: 'field',
    value: '_time',
  },
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

export const buildTableQueryConfig = (bucket: Bucket): QueryConfig => {
  const id = uuid.v4()
  const {name, rp} = bucket

  return {
    ...defaultQueryConfig,
    id,
    database: name,
    retentionPolicy: rp,
    groupBy: {tags: []},
    fields: tableFields,
    fill: null,
  }
}

export const buildLogQuery = (
  type: Query.TypeEnum,
  searchParams: LogSearchParams
): string => {
  switch (type) {
    case Query.TypeEnum.Influxql:
      return `${buildInfluxQLQuery(searchParams)} ORDER BY time DESC`
    case Query.TypeEnum.Flux:
      return buildFluxQuery(searchParams)
  }
}

export const validateTailQuery = (
  logQuery: LogQuery,
  logTailID: number,
  currentTailID: number
) => ({
  logQuery,
  error:
    isCurrentID(logTailID, currentTailID, 'Stale log tail') ||
    hasLogQueryParams(
      logQuery,
      `Missing params required to fetch tail logs. Maybe there's a race condition with setting buckets?`
    ),
})

export const validateOlderQuery = (
  logQuery: LogQuery,
  logOlderBatchID: string,
  currentOlderBatchID: string
) => ({
  logQuery,
  error:
    isCurrentID(
      logOlderBatchID,
      currentOlderBatchID,
      'Stale older batch request'
    ) ||
    hasLogQueryParams(
      logQuery,
      `Missing params required to fetch older logs. Maybe there's a race condition with setting namespaces?`
    ),
})

const isCurrentID = <T>(id: T, currentID: T, message: string) => {
  if (!id || id !== currentID) {
    return message
  }

  return null
}

const hasLogQueryParams = (
  {source, config, lower, upper, filters}: LogQuery,
  message: string
) => {
  if (!_.every([source, config, lower, upper, filters])) {
    return message
  }

  return null
}
