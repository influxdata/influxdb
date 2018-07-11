import {proxy} from 'src/utils/queryUrlGenerator'
import {makeQueryForTemplate} from 'src/dashboards/utils/tempVars'
import {parseMetaQuery} from 'src/tempVars/parsing'
import templateReplace from 'src/tempVars/utils/replace'

import {TEMPLATE_VARIABLE_TYPES} from 'src/tempVars/constants'

import {Template} from 'src/types'

export const hydrateTemplate = async (
  proxyLink: string,
  template: Template,
  templates: Template[]
): Promise<Template> => {
  if (!template.query || !template.query.influxql) {
    return template
  }

  const query = templateReplace(
    makeQueryForTemplate(template.query),
    templates.filter(t => !isTemplateNested(t))
  )

  const response = await proxy({source: proxyLink, query})
  const values = parseMetaQuery(query, response.data)
  const type = TEMPLATE_VARIABLE_TYPES[template.type]
  const selectedValue = getSelectedValue(template)
  const selectedLocalValue = getLocalSelectedValue(template)

  const templateValues = values.map(value => {
    return {
      type,
      value,
      selected: value === selectedValue,
      localSelected: value === selectedLocalValue,
    }
  })

  if (templateValues.length && !templateValues.find(v => v.selected)) {
    // Handle stale selected value
    templateValues[0].selected = true
  }

  return {...template, values: templateValues}
}

export const isTemplateNested = (template: Template): boolean => {
  // A _nested template_ is one whose query references other templates
  return (
    template.query &&
    template.query.influxql &&
    !!makeQueryForTemplate(template.query).match(/(.*:.+:.*)+/)
  )
}

const getSelectedValue = (template: Template): string | false => {
  const selected = template.values.find(v => v.selected)

  return selected ? selected.value : false
}

const getLocalSelectedValue = (template: Template): string | false => {
  const selected = template.values.find(v => v.localSelected)

  return selected ? selected.value : false
}
