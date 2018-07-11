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

const TEMPLATES_SORTING_ORDER = {
  [TemplateType.CSV]: 0,
  [TemplateType.Map]: 0,
  [TemplateType.AutoGroupBy]: 1,
  [TemplateType.Constant]: 1,
  [TemplateType.FieldKeys]: 1,
  [TemplateType.Measurements]: 1,
  [TemplateType.TagKeys]: 1,
  [TemplateType.TagValues]: 1,
  [TemplateType.Databases]: 1,
  [TemplateType.MetaQuery]: 1,
}

const sortTemplates = (a: Template, b: Template): number => {
  return TEMPLATES_SORTING_ORDER[a.type] - TEMPLATES_SORTING_ORDER[b.type]
}

const templateReplace = (query: string, tempVars: Template[]) => {
  const sortedTempVars = [...tempVars].sort(sortTemplates)

  return sortedTempVars.reduce(
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

export default templateReplace
