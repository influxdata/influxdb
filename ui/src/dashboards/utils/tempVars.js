import _ from 'lodash'
import queryString from 'query-string'

import {TEMPLATE_VARIABLE_QUERIES} from 'src/dashboards/constants'

const generateTemplateVariableQuery = ({
  type,
  query: {
    database,
    // rp, TODO
    measurement,
    tagKey,
  },
}) => {
  const tempVars = []

  if (database) {
    tempVars.push({
      tempVar: ':database:',
      values: [
        {
          type: 'database',
          value: database,
        },
      ],
    })
  }
  if (measurement) {
    tempVars.push({
      tempVar: ':measurement:',
      values: [
        {
          type: 'measurement',
          value: measurement,
        },
      ],
    })
  }
  if (tagKey) {
    tempVars.push({
      tempVar: ':tagKey:',
      values: [
        {
          type: 'tagKey',
          value: tagKey,
        },
      ],
    })
  }

  const query = TEMPLATE_VARIABLE_QUERIES[type]

  return {
    query,
    tempVars,
  }
}

export const makeQueryForTemplate = ({influxql, db, measurement, tagKey}) =>
  influxql
    .replace(':database:', `"${db}"`)
    .replace(':measurement:', `"${measurement}"`)
    .replace(':tagKey:', `"${tagKey}"`)

export const stripTempVar = tempVarName =>
  tempVarName.substr(1, tempVarName.length - 2)

export const generateURLQueryFromTempVars = tempVars => {
  const urlQueries = {}

  tempVars.forEach(({tempVar, values}) => {
    const selected = values.find(value => value.selected === true)
    const strippedTempVar = stripTempVar(tempVar)

    urlQueries[strippedTempVar] = selected.value
  })

  return urlQueries
}

export const isValidTempVarOverride = (values, overrideValue) =>
  !!values.find(({value}) => value === overrideValue)

const reconcileTempVarsWithOverrides = (currentTempVars, tempVarOverrides) => {
  if (!tempVarOverrides) {
    return currentTempVars
  }
  const reconciledTempVars = currentTempVars.map(tempVar => {
    const {tempVar: name, values} = tempVar
    const strippedTempVar = stripTempVar(name)
    const overrideValue = tempVarOverrides[strippedTempVar]

    if (overrideValue) {
      const isValid = isValidTempVarOverride(values, overrideValue)

      if (isValid) {
        const overriddenValues = values.map(tempVarValue => {
          const {value} = tempVarValue
          if (value === overrideValue) {
            return {...tempVarValue, selected: true}
          }
          return {...tempVarValue, selected: false}
        })
        return {...tempVar, values: overriddenValues}
      }

      return tempVar
    }

    return tempVar
  })

  return reconciledTempVars
}

export const applyDashboardTempVarOverrides = (
  dashboard,
  tempVarOverrides
) => ({
  ...dashboard,
  templates: reconcileTempVarsWithOverrides(
    dashboard.templates,
    tempVarOverrides
  ),
})

export const getInvalidTempVarsInURLQuery = tempVars => {
  const urlQueries = queryString.parse(window.location.search)

  const urlQueryTempVarsWithInvalidValues = _.reduce(
    urlQueries,
    (acc, v, k) => {
      const matchedTempVar = tempVars.find(
        ({tempVar}) => stripTempVar(tempVar) === k
      )
      if (matchedTempVar) {
        const isValidTempVarValue = !!matchedTempVar.values.find(
          ({value}) => value === v
        )
        if (!isValidTempVarValue) {
          acc.push({key: k, value: v})
        }
      }
      return acc
    },
    []
  )

  return urlQueryTempVarsWithInvalidValues
}

export default generateTemplateVariableQuery
