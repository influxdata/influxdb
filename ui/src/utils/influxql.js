import _ from 'lodash'

import {TEMP_VAR_INTERVAL, AUTO_GROUP_BY} from 'shared/constants'
import {NULL_STRING} from 'shared/constants/queryFillOptions'
import {
  TYPE_QUERY_CONFIG,
  TYPE_SHIFTED,
  TYPE_IFQL,
} from 'src/dashboards/constants'
import {shiftTimeRange} from 'shared/query/helpers'

/* eslint-disable quotes */
export const quoteIfTimestamp = ({lower, upper}) => {
  if (lower && lower.includes('Z') && !lower.includes("'")) {
    lower = `'${lower}'`
  }

  if (upper && upper.includes('Z') && !upper.includes("'")) {
    upper = `'${upper}'`
  }

  return {lower, upper}
}
/* eslint-enable quotes */

export default function buildInfluxQLQuery(timeRange, config, shift) {
  const {groupBy, fill = NULL_STRING, tags, areTagsAccepted} = config
  const {upper, lower} = quoteIfTimestamp(timeRange)

  const select = _buildSelect(config, shift)
  if (select === null) {
    return null
  }

  const condition = _buildWhereClause({lower, upper, tags, areTagsAccepted})
  const dimensions = _buildGroupBy(groupBy)
  const fillClause = groupBy.time ? _buildFill(fill) : ''

  return `${select}${condition}${dimensions}${fillClause}`
}

function _buildSelect({fields, database, retentionPolicy, measurement}, shift) {
  if (!database || !measurement || !fields || !fields.length) {
    return null
  }

  const rpSegment = retentionPolicy ? `"${retentionPolicy}"` : ''
  const fieldsClause = _buildFields(fields, shift)
  const fullyQualifiedMeasurement = `"${database}".${rpSegment}."${measurement}"`
  const statement = `SELECT ${fieldsClause} FROM ${fullyQualifiedMeasurement}`
  return statement
}

// type arg will reason about new query types i.e. IFQL, GraphQL, or queryConfig
export const buildQuery = (type, timeRange, config, shift) => {
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

    case TYPE_IFQL: {
      // build query usining IFQL here
    }
  }

  return buildInfluxQLQuery(timeRange, config)
}

export function buildSelectStatement(config) {
  return _buildSelect(config)
}

function _buildFields(fieldFuncs, shift = '') {
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
          const args = _buildFields(f.args)
          const alias = f.alias ? ` AS "${f.alias}${shift}"` : ''
          return `${f.value}(${args})${alias}`
        }
      }
    })
    .join(', ')
}

function _buildWhereClause({lower, upper, tags, areTagsAccepted}) {
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

function _buildGroupBy(groupBy) {
  return `${_buildGroupByTime(groupBy)}${_buildGroupByTags(groupBy)}`
}

function _buildGroupByTime(groupBy) {
  if (!groupBy || !groupBy.time) {
    return ''
  }

  return ` GROUP BY time(${groupBy.time === AUTO_GROUP_BY
    ? TEMP_VAR_INTERVAL
    : `${groupBy.time}`})`
}

function _buildGroupByTags(groupBy) {
  if (!groupBy || !groupBy.tags.length) {
    return ''
  }

  const tags = groupBy.tags.map(t => `"${t}"`).join(', ')

  if (groupBy.time) {
    return `, ${tags}`
  }

  return ` GROUP BY ${tags}`
}

function _buildFill(fill) {
  return ` FILL(${fill})`
}

export const buildRawText = (q, timeRange) =>
  q.rawText || buildInfluxQLQuery(timeRange, q) || ''
