import {topologicalSort, graphFromTemplates} from 'src/tempVars/utils/graph'

import {
  Template,
  TemplateType,
  TemplateValueType,
  TemplateValue,
} from 'src/types/tempVars'
import {
  TEMP_VAR_INTERVAL,
  DEFAULT_PIXELS,
  DEFAULT_DURATION_MS,
} from 'src/shared/constants'

function sortTemplates(templates: Template[]): Template[] {
  const graph = graphFromTemplates(templates)

  return topologicalSort(graph).map(t => t.initialTemplate)
}

export const replaceInterval = (
  query: string,
  pixels: number,
  durationMs: number
) => {
  if (!query.includes(TEMP_VAR_INTERVAL)) {
    return query
  }

  if (!pixels) {
    pixels = DEFAULT_PIXELS
  }

  if (!durationMs) {
    durationMs = DEFAULT_DURATION_MS
  }

  // duration / width of visualization in pixels
  const msPerPixel = Math.floor(durationMs / pixels)

  return replaceAll(query, TEMP_VAR_INTERVAL, `${msPerPixel}ms`)
}

const templateReplace = (query: string, templates: Template[]) => {
  const sortedTemplates = sortTemplates(templates)

  return sortedTemplates.reduce(
    (acc, template) => renderTemplate(acc, template),
    query
  )
}

const renderTemplate = (query: string, template: Template): string => {
  if (!template.values.length) {
    return query
  }

  if (query && !query.includes(template.tempVar)) {
    return query
  }

  const localSelectedTemplateValue: TemplateValue = template.values.find(
    v => v.localSelected
  )
  const selectedTemplateValue: TemplateValue = template.values.find(
    v => v.selected
  )

  const templateValue = localSelectedTemplateValue || selectedTemplateValue
  if (!templateValue) {
    return query
  }

  const {tempVar} = template
  const {value, type} = templateValue

  let q = ''

  // First replace all template variable types in regular expressions.  Values should appear unquoted.
  switch (type) {
    case TemplateValueType.TagKey:
    case TemplateValueType.FieldKey:
    case TemplateValueType.Measurement:
    case TemplateValueType.Database:
    case TemplateValueType.TagValue:
    case TemplateValueType.TimeStamp:
      q = replaceAllRegex(query, tempVar, value)
      break
    default:
      q = query
  }

  // Then render template variables not in regular expressions
  switch (type) {
    case TemplateValueType.TagKey:
    case TemplateValueType.FieldKey:
    case TemplateValueType.Measurement:
    case TemplateValueType.Database:
      return replaceAll(q, tempVar, `"${value}"`)
    case TemplateValueType.TagValue:
    case TemplateValueType.TimeStamp:
      return replaceAll(q, tempVar, `'${value}'`)
    case TemplateValueType.CSV:
    case TemplateValueType.Constant:
    case TemplateValueType.MetaQuery:
    case TemplateValueType.Map:
      return replaceAll(q, tempVar, value)
    default:
      return query
  }
}

const replaceAllRegex = (
  query: string,
  search: string,
  replacement: string
) => {
  // check for presence of anything between two forward slashes /[your stuff here]/
  const matches = query.match(/\/([^\/]*)\//gm)

  if (!matches) {
    return query
  }

  return matches.reduce((acc, m) => {
    if (m.includes(search)) {
      const replaced = m.replace(search, replacement)
      return acc.split(m).join(replaced)
    }

    return acc
  }, query)
}

const replaceAll = (query: string, search: string, replacement: string) => {
  return query.split(search).join(replacement)
}

export const templateInternalReplace = (template: Template): string => {
  const {influxql, db, measurement, tagKey} = template.query

  if (template.type === TemplateType.MetaQuery) {
    // A custom meta query template may reference other templates whose names
    // conflict with the `database`, `measurement` and `tagKey` fields stored
    // within a template's `query` object. Since these fields are always empty
    // for a custom meta query template, we do not attempt to replace them
    return influxql
  }

  return influxql
    .replace(':database:', `"${db}"`)
    .replace(':measurement:', `"${measurement}"`)
    .replace(':tagKey:', `"${tagKey}"`)
}

export default templateReplace
