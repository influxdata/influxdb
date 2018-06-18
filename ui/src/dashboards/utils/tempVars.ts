import _ from 'lodash'

import {
  Dashboard,
  Template,
  TemplateQuery,
  TemplateValue,
  URLQueryParams,
} from 'src/types'
import {TemplateUpdate} from 'src/types'

export const makeQueryForTemplate = ({
  influxql,
  db,
  measurement,
  tagKey,
}: TemplateQuery): string =>
  influxql
    .replace(':database:', `"${db}"`)
    .replace(':measurement:', `"${measurement}"`)
    .replace(':tagKey:', `"${tagKey}"`)

export const stripTempVar = (tempVarName: string): string =>
  tempVarName.substr(1, tempVarName.length - 2)

export const generateURLQueryParamsFromTempVars = (
  tempVars: Template[]
): URLQueryParams => {
  const urlQueryParams = {}

  tempVars.forEach(({tempVar, values}) => {
    const selected = values.find(value => value.selected === true)
    const strippedTempVar = stripTempVar(tempVar)

    urlQueryParams[strippedTempVar] = selected.value
  })

  return urlQueryParams
}

const isValidTempVarOverride = (
  values: TemplateValue[],
  overrideValue: string
): boolean => !!values.find(({value}) => value === overrideValue)

const reconcileTempVarsWithOverrides = (
  currentTempVars: Template[],
  tempVarOverrides: URLQueryParams
): Template[] => {
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
  dashboard: Dashboard,
  tempVarOverrides: URLQueryParams
): Dashboard => ({
  ...dashboard,
  templates: reconcileTempVarsWithOverrides(
    dashboard.templates,
    tempVarOverrides
  ),
})

export const findUpdatedTempVarsInURLQueryParams = (
  tempVars: Template[],
  urlQueryParams: URLQueryParams
): TemplateUpdate[] => {
  const urlQueryParamsTempVarsWithInvalidValues = _.reduce(
    urlQueryParams,
    (acc, v, k) => {
      const matchedTempVar = tempVars.find(
        ({tempVar}) => stripTempVar(tempVar) === k
      )
      if (matchedTempVar) {
        const isDifferentTempVarValue = !!matchedTempVar.values.find(
          ({value, selected}) => selected && value !== v
        )
        if (isDifferentTempVarValue) {
          acc.push({key: k, value: v})
        }
      }
      return acc
    },
    []
  )

  return urlQueryParamsTempVarsWithInvalidValues
}

export const findInvalidTempVarsInURLQuery = (
  tempVars: Template[],
  urlQueryParams: URLQueryParams
): TemplateUpdate[] => {
  const urlQueryParamsTempVarsWithInvalidValues = _.reduce(
    urlQueryParams,
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

  return urlQueryParamsTempVarsWithInvalidValues
}
