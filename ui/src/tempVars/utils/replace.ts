import {Template, TemplateValueType, TemplateValue} from 'src/types/tempVars'

const templateReplace = (q: string, tempVars: Template[]) => {
  const query = tempVars.reduce((acc, template) => {
    const qu = renderTemplate(acc, template)
    return qu
  }, q)

  return query
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

  switch (type) {
    case TemplateValueType.TagKey:
    case TemplateValueType.FieldKey:
    case TemplateValueType.Measurement:
    case TemplateValueType.Database:
      return replaceAll(query, tempVar, `"${value}"`)
    case TemplateValueType.TagValue:
    case TemplateValueType.TimeStamp:
      return replaceAll(query, tempVar, `'${value}'`)
    case TemplateValueType.CSV:
    case TemplateValueType.Constant:
    case TemplateValueType.MetaQuery:
      return replaceAll(query, tempVar, value)
    default:
      return query
  }
}

const replaceAll = (query: string, search: string, replacement: string) => {
  return query.split(search).join(replacement)
}

export default templateReplace
