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
    const localSelected = values.find(value => value.localSelected === true)
    const strippedTempVar = stripTempVar(tempVar)

    urlQueryParams[strippedTempVar] = _.get(localSelected, 'value', '')
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
    if (overrideValue && isValidTempVarOverride(values, overrideValue)) {
      const overriddenValues = values.map(tempVarValue => {
        const {value} = tempVarValue
        if (value === overrideValue) {
          return {...tempVarValue, localSelected: true}
        }
        return {...tempVarValue, localSelected: false}
      })
      return {...tempVar, values: overriddenValues}
    } else {
      // or pick selected value.
      const valuesWithLocalSelected = values.map(tempVarValue => {
        const isSelected = tempVarValue.selected
        return {...tempVarValue, localSelected: isSelected}
      })
      return {...tempVar, values: valuesWithLocalSelected}
    }
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

  return makeLocalSelected(template, selectedValue)
}

export const makeLocalSelected = (
  template: Template,
  value: string
): Template => {
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

  const valuesWithLocalSelected = template.values.map(v => {
    if (v.value === valueToChoose) {
      return {...v, localSelected: true}
    } else {
      return {...v, localSelected: false}
    }
  })

  return {...template, values: valuesWithLocalSelected}
}

export const reconcileSelectedAndLocalSelectedValues = (
  nextTemplate: Template,
  nextNextTemplate: Template
): Template => {
  const localSelectedValue = nextTemplate.values.find(v => v.localSelected)
  const selectedValue = nextTemplate.values.find(v => v.selected)
  const templateWithLocalSelected = makeSelected(
    nextNextTemplate,
    getDeep<string>(selectedValue, 'value', '')
  )

  const templateWithLocalSelectedAndSelected = makeLocalSelected(
    templateWithLocalSelected,
    getDeep<string>(localSelectedValue, 'value', '')
  )

  return templateWithLocalSelectedAndSelected
}
