// Libraries
import uuid from 'uuid'

// Types
import {Bucket} from 'src/types/v2/buckets'
import {QueryConfig, Field} from 'src/types'
import {Filter} from 'src/types/logs'

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

const PIPE = '\n  |> '
const ROW_NAME = 'r'
const SORT_FUNC = ['sort(cols: ["_time"])']

export function buildInfiniteScrollLogQuery(
  lower: string,
  upper: string,
  config: QueryConfig,
  filters: Filter[]
) {
  const {database, retentionPolicy, fields, measurement} = config
  const bucketName = `"${database}/${retentionPolicy}"`

  return buildRowsQuery(bucketName, measurement, lower, upper)
    .concat(buildFilterFunc(filters), SORT_FUNC, buildFieldsMapFunc(fields))
    .join(PIPE)
}

const buildRowsQuery = (
  bucketName: string,
  measurement: string,
  lower: string,
  upper: string
): string[] => {
  return [
    `from(bucket: ${bucketName})`,
    `range(start: ${lower}, stop: ${upper})`,
    `filter(fn: (${ROW_NAME}) => ${ROW_NAME}._measurement == "${measurement}")`,
    `pivot(rowKey:["_time"], colKey: ["_field"], valueCol: "_value")`,
    `group(none: true)`,
  ]
}

const buildFieldsMapFunc = (fields: Field[]): string[] => {
  const fieldNames = fields
    .map(field => `${field.alias}: ${ROW_NAME}.${field.value}`)
    .join(', ')

  return [`map(fn: (${ROW_NAME}) => ({${fieldNames}}))`]
}

const buildFilterFunc = (filters: Filter[]): string[] => {
  if (filters.length === 0) {
    return []
  }

  const filterConditions = filters
    .map(filterToPredicateExpression)
    .join(' and ')

  return [`filter(fn: (${ROW_NAME}) => ${filterConditions})`]
}

const filterToPredicateExpression = (filter: Filter): string => {
  switch (filter.operator) {
    case '!~':
    case '=~':
      return `${ROW_NAME}.${filter.key} ${filter.operator} /${filter.value}/`
    default:
      return `${ROW_NAME}.${filter.key} ${filter.operator} "${filter.value}"`
  }
}
