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
    const picked = values.find(value => value.picked === true)
    const strippedTempVar = stripTempVar(tempVar)

    urlQueryParams[strippedTempVar] = _.get(picked, 'value', '')
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
            return {...tempVarValue, picked: true}
          }
          return {...tempVarValue, picked: false}
        })
        return {...tempVar, values: overriddenValues}
      }
      // or pick selected value.
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

const makeSelected = (template: Template, value: string): Template => {
  const found = template.values.find(v => v.value === value)

  let valueToChoose
  if (found) {
    valueToChoose = found.value
  } else {
    valueToChoose = getDeep<string>(template, 'values.0.value', '')
  }

  const valuesWithSelected = template.values.map(v => {
    if (v.value === valueToChoose) {
      return {...v, selected: true}
    } else {
      return {...v, selected: false}
    }
  })

  return {...template, values: valuesWithSelected}
}

export const pickSelected = (template: Template): Template => {
  const selectedValue = ''

  return makePicked(template, selectedValue)
}

export const makePicked = (template: Template, value: string): Template => {
  const found = template.values.find(v => v.value === value)
  const selectedValue = template.values.find(v => v.selected)

  let valueToChoose: string
  if (found) {
    valueToChoose = found.value
  } else if (selectedValue) {
    valueToChoose = selectedValue.value
  } else {
    valueToChoose = getDeep<string>(template, 'values.0.value', '')
  }

  const valuesWithPicked = template.values.map(v => {
    if (v.value === valueToChoose) {
      return {...v, picked: true}
    } else {
      return {...v, picked: false}
    }
  })

  return {...template, values: valuesWithPicked}
}

export const reconcileSelectedAndPickedValues = (
  nextTemplate: Template,
  nextNextTemplate: Template
): Template => {
  const pickedValue = nextTemplate.values.find(v => v.picked)
  const selectedValue = nextTemplate.values.find(v => v.selected)
  // make picked from selected
  const TemplateWithPicked = makeSelected(
    nextNextTemplate,
    getDeep<string>(selectedValue, 'value', '')
  )

  const TemplateWithPickedAndSelected = makePicked(
    TemplateWithPicked,
    getDeep<string>(pickedValue, 'value', '')
  )

  return TemplateWithPickedAndSelected
}
