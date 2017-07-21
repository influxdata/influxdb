import _ from 'lodash'

import {
  TEMP_VAR_INTERVAL,
  DEFAULT_DASHBOARD_GROUP_BY_INTERVAL,
} from 'shared/constants'

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

export default function buildInfluxQLQuery(timeBounds, config) {
  const {groupBy, tags, areTagsAccepted} = config
  const {upper, lower} = quoteIfTimestamp(timeBounds)

  const select = _buildSelect(config)
  if (select === null) {
    return null
  }

  const condition = _buildWhereClause({lower, upper, tags, areTagsAccepted})
  const dimensions = _buildGroupBy(groupBy)

  return `${select}${condition}${dimensions}`
}

function _buildSelect({fields, database, retentionPolicy, measurement}) {
  if (!database || !measurement || !fields || !fields.length) {
    return null
  }

  const rpSegment = retentionPolicy ? `"${retentionPolicy}"` : ''
  const fieldsClause = _buildFields(fields)
  const fullyQualifiedMeasurement = `"${database}".${rpSegment}."${measurement}"`
  const statement = `SELECT ${fieldsClause} FROM ${fullyQualifiedMeasurement}`
  return statement
}

export function buildSelectStatement(config) {
  return _buildSelect(config)
}

function _buildFields(fieldFuncs) {
  const hasAggregate = fieldFuncs.some(f => f.funcs && f.funcs.length)
  if (hasAggregate) {
    return fieldFuncs
      .map(f => {
        return f.funcs
          .map(func => `${func}("${f.field}") AS "${func}_${f.field}"`)
          .join(', ')
      })
      .join(', ')
  }

  return fieldFuncs
    .map(f => {
      return f.field === '*' ? '*' : `"${f.field}"`
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

  return ` GROUP BY ${groupBy.time === DEFAULT_DASHBOARD_GROUP_BY_INTERVAL
    ? TEMP_VAR_INTERVAL
    : `time(${groupBy.time})`}`
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
