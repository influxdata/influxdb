import _ from 'lodash'

import {TEMP_VAR_INTERVAL, AUTO_GROUP_BY} from 'shared/constants'
import {NULL_STRING} from 'shared/constants/queryFillOptions'
import {
  TYPE_QUERY_CONFIG,
  TYPE_SHIFTED,
  TYPE_IFQL,
} from 'src/dashboards/constants'
import {shiftTimeRange} from 'shared/query/helpers'
import timeRanges from 'hson!shared/data/timeRanges.hson'

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
      const {multiple, unit} = shift
      return buildInfluxQLQuery(
        shiftTimeRange(timeRange, shift),
        config,
        `_shifted__${multiple}__${unit}`
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

  return ` GROUP BY ${groupBy.time === AUTO_GROUP_BY
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

function _buildFill(fill) {
  return ` FILL(${fill})`
}

const buildCannedDashboardQuery = (query, {lower, upper}, host) => {
  const {defaultGroupBy} = timeRanges.find(range => range.lower === lower) || {
    defaultGroupBy: '5m',
  }
  const {wheres, groupbys} = query

  let text = query.text

  if (upper) {
    text += ` where time > '${lower}' AND time < '${upper}'`
  } else {
    text += ` where time > ${lower}`
  }

  if (host) {
    text += ` and \"host\" = '${host}'`
  }

  if (wheres && wheres.length > 0) {
    text += ` and ${wheres.join(' and ')}`
  }

  if (groupbys) {
    if (groupbys.find(g => g.includes('time'))) {
      text += ` group by ${groupbys.join(',')}`
    } else if (groupbys.length > 0) {
      text += ` group by time(${defaultGroupBy}),${groupbys.join(',')}`
    } else {
      text += ` group by time(${defaultGroupBy})`
    }
  } else {
    text += ` group by time(${defaultGroupBy})`
  }

  return text
}

export const buildQueriesForLayouts = (cell, source, timeRange, host) => {
  return cell.queries.map(query => {
    let queryText
    // Canned dashboards use an different a schema different from queryConfig.
    if (query.queryConfig) {
      const {queryConfig: {rawText, range}} = query
      const tR = range || {
        upper: ':upperDashboardTime:',
        lower: ':dashboardTime:',
      }
      queryText = rawText || buildInfluxQLQuery(tR, query.queryConfig)
    } else {
      queryText = buildCannedDashboardQuery(query, timeRange, host)
    }

    return {...query, host: source.links.proxy, text: queryText}
  })
}

export const buildRawText = (q, timeRange) =>
  q.rawText || buildInfluxQLQuery(timeRange, q) || ''
