import _ from 'lodash'
import {getDeep} from 'src/utils/wrappers'

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

    urlQueryParams[strippedTempVar] = _.get(selected, 'value', '')
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

const makeDefault = (template: Template, value: string): Template => {
  const found = template.values.find(v => v.value === value)

  let valueToChoose
  if (found) {
    valueToChoose = found.value
  } else {
    valueToChoose = getDeep<string>(template, 'values.0.value', '')
  }

  const valuesWithDefault = template.values.map(v => {
    if (v.value === valueToChoose) {
      return {...v, default: true}
    } else {
      return {...v, default: false}
    }
  })

  return {...template, values: valuesWithDefault}
}

const makeSelected = (template: Template, value: string): Template => {
  const found = template.values.find(v => v.value === value)
  const defaultValue = template.values.find(v => v.default)

  let valueToChoose
  if (found) {
    valueToChoose = found.value
  } else if (defaultValue) {
    valueToChoose = defaultValue
  } else {
    valueToChoose = getDeep<string>(template, 'values.0.value', '')
  }

  const valuesWithDefault = template.values.map(v => {
    if (v.value === valueToChoose) {
      return {...v, selected: true}
    } else {
      return {...v, selected: false}
    }
  })

  return {...template, values: valuesWithDefault}
}

export const reconcileDefaultAndSelectedValues = (
  nextTemplate: Template,
  nextNextTemplate: Template
): Template => {
  const selectedValue = nextTemplate.values.find(v => v.selected)
  const defaultValue = nextTemplate.values.find(v => v.default)
  // make selected from default
  const TemplateWithDefault = makeDefault(
    nextNextTemplate,
    getDeep<string>(defaultValue, 'value', '')
  )

  const TemplateWithDefaultAndSelected = makeSelected(
    TemplateWithDefault,
    getDeep<string>(selectedValue, 'value', '')
  )
  return TemplateWithDefaultAndSelected
}
