import {Template, TemplateValueType, TemplateValue} from 'src/types/tempVars'
import {TEMP_VAR_INTERVAL} from 'src/shared/constants'

export const intervalReplace = (
  query: string,
  pixels: number,
  durationMs: number
) => {
  if (!query.includes(TEMP_VAR_INTERVAL)) {
    return query
  }

  // duration / width of visualization in pixels
  const msPerPixel = Math.floor(durationMs / pixels)

  return replaceAll(query, TEMP_VAR_INTERVAL, `${msPerPixel}ms`)
}

const templateReplace = (query: string, tempVars: Template[]) => {
  const replacedQuery = tempVars.reduce((acc, template) => {
    return renderTemplate(acc, template)
  }, query)

  return replacedQuery
}

const renderTemplate = (query: string, template: Template): string => {
  if (!template.values.length) {
    return query
  }

  if (query && !query.includes(template.tempVar)) {
    return query
  }

  const templateValue: TemplateValue = template.values.find(v => v.selected)

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
  const matches = query.match(/\/([^\/]*)\//gm)
  const isReplaceable = !!matches && matches.some(m => m.includes(search))

  if (!isReplaceable) {
    return query
  }

  return replaceAll(query, search, replacement)
}

const replaceAll = (query: string, search: string, replacement: string) => {
  return query.split(search).join(replacement)
}

export default templateReplace
