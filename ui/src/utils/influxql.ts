import _ from 'lodash'

import {TEMP_VAR_INTERVAL, AUTO_GROUP_BY} from 'src/shared/constants'
import {NULL_STRING} from 'src/shared/constants/queryFillOptions'
import {
  TYPE_QUERY_CONFIG,
  TYPE_SHIFTED,
  TYPE_FLUX,
} from 'src/dashboards/constants'
import {shiftTimeRange} from 'src/shared/query/helpers'
import {QueryConfig, Field, GroupBy, TimeShift, TimeRange} from 'src/types'

export const quoteIfTimestamp = ({lower, upper}: TimeRange): TimeRange => {
  if (lower && lower.includes('Z') && !lower.includes("'")) {
    lower = `'${lower}'`
  }

  if (upper && upper.includes('Z') && !upper.includes("'")) {
    upper = `'${upper}'`
  }

  return {lower, upper}
}

export default function buildInfluxQLQuery(
  timeRange: TimeRange,
  config: QueryConfig,
  shift: string = ''
): string {
  const {groupBy, fill = NULL_STRING, tags, areTagsAccepted} = config
  const {upper, lower} = quoteIfTimestamp(timeRange)

  const select = buildSelect(config, shift)
  if (select === null) {
    return null
  }

  const condition = buildWhereClause({lower, upper, tags, areTagsAccepted})
  const dimensions = buildGroupBy(groupBy)
  const fillClause = groupBy.time ? buildFill(fill) : ''

  return `${select}${condition}${dimensions}${fillClause}`
}

export function buildSelect(
  {fields, database, retentionPolicy, measurement}: QueryConfig,
  shift: string | null = null
): string {
  if (!database || !measurement || _.isEmpty(fields)) {
    return null
  }

  const rpSegment = retentionPolicy ? `"${retentionPolicy}"` : ''
  const fieldsClause = buildFields(fields, shift)
  const fullyQualifiedMeasurement = `"${database}".${rpSegment}."${measurement}"`
  const statement = `SELECT ${fieldsClause} FROM ${fullyQualifiedMeasurement}`
  return statement
}

// type arg will reason about new query types i.e. Flux, GraphQL, or queryConfig
export const buildQuery = (
  type: string,
  timeRange: TimeRange,
  config: QueryConfig,
  shift: TimeShift | null = null
): string => {
  switch (type) {
    case TYPE_QUERY_CONFIG: {
      return buildInfluxQLQuery(timeRange, config)
    }
    case TYPE_SHIFTED: {
      const {quantity, unit} = shift
      return buildInfluxQLQuery(
        shiftTimeRange(timeRange, shift),
        config,
        `_shifted__${quantity}__${unit}`
      )
    }

    case TYPE_FLUX: {
      // build query usining FLUX here
    }
  }

  return buildInfluxQLQuery(timeRange, config)
}

export function buildSelectStatement(config: QueryConfig): string {
  return buildSelect(config)
}

function buildFields(fieldFuncs: Field[], shift = ''): string {
  if (!fieldFuncs) {
    return ''
  }

  return fieldFuncs
    .map(f => {
      switch (f.type) {
        case 'field': {
          return f.value === '*' ? '*' : `"${f.value}"`
        }
        case 'wildcard': {
          return '*'
        }
        case 'regex': {
          return `/${f.value}/`
        }
        case 'number': {
          return `${f.value}`
        }
        case 'integer': {
          return `${f.value}`
        }
        case 'func': {
          const args = buildFields(f.args)
          const alias = f.alias ? ` AS "${f.alias}${shift}"` : ''
          return `${f.value}(${args})${alias}`
        }
      }
    })
    .join(', ')
}

export function buildWhereClause({
  lower,
  upper,
  tags,
  areTagsAccepted,
}: QueryConfig): string {
  const timeClauses = []

  const timeClause = quoteIfTimestamp({lower, upper})

  if (timeClause.lower) {
    timeClauses.push(`time > ${lower}`)
  }

  if (timeClause.upper) {
    timeClauses.push(`time < ${upper}`)
  }

  // If a tag key has more than one value, * e.g. cpu=cpu1, cpu=cpu2, combine
  // them with OR instead of AND for the final query.
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

export function buildGroupBy(groupBy: GroupBy): string {
  return `${buildGroupByTime(groupBy)}${buildGroupByTags(groupBy)}`
}

function buildGroupByTime(groupBy: GroupBy): string {
  if (!groupBy || !groupBy.time) {
    return ''
  }

  return ` GROUP BY time(${
    groupBy.time === AUTO_GROUP_BY ? TEMP_VAR_INTERVAL : `${groupBy.time}`
  })`
}

function buildGroupByTags(groupBy: GroupBy): string {
  if (!groupBy || !groupBy.tags.length) {
    return ''
  }

  const tags = groupBy.tags.map(t => `"${t}"`).join(', ')

  if (groupBy.time) {
    return `, ${tags}`
  }

  return ` GROUP BY ${tags}`
}

export function buildFill(fill: string): string {
  return ` FILL(${fill})`
}

export const buildRawText = (
  config: QueryConfig,
  timeRange: TimeRange
): string => config.rawText || buildInfluxQLQuery(timeRange, config) || ''
