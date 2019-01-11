// Types
import {Field} from 'src/types'
import {Filter, LogSearchParams} from 'src/types/logs'

const PIPE = ' |> '
const ROW_NAME = 'r'
const SORT_FUNC = ['sort(columns: ["_time"])']

export function buildFluxQuery({
  lower,
  upper,
  config,
  filters,
}: LogSearchParams) {
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
    `pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")`,
    `group()`,
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
