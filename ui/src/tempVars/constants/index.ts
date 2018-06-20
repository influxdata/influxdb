import uuid from 'uuid'

import {TimeRange} from 'src/types/query'
import {TEMP_VAR_DASHBOARD_TIME} from 'src/shared/constants'
import {Template, TemplateType, TemplateValueType, BuilderType} from 'src/types'

interface TemplateTypesListItem {
  text: string
  type: TemplateType
  builderType: BuilderType
}

export const TEMPLATE_TYPES_LIST: TemplateTypesListItem[] = [
  {
    text: 'Databases',
    type: TemplateType.Databases,
    builderType: BuilderType.Databases,
  },
  {
    text: 'Measurements',
    type: TemplateType.Measurements,
    builderType: BuilderType.Measurements,
  },
  {
    text: 'Field Keys',
    type: TemplateType.FieldKeys,
    builderType: BuilderType.FieldKeys,
  },
  {
    text: 'Tag Keys',
    type: TemplateType.TagKeys,
    builderType: BuilderType.TagKeys,
  },
  {
    text: 'Tag Values',
    type: TemplateType.TagValues,
    builderType: BuilderType.TagValues,
  },
  {
    text: 'CSV (Manual Entry)',
    type: TemplateType.CSV,
    builderType: BuilderType.CSVManual,
  },
  {
    text: 'CSV',
    type: TemplateType.CSV,
    builderType: BuilderType.CSVFile,
  },
  {
    text: 'Custom Meta Query',
    type: TemplateType.MetaQuery,
    builderType: BuilderType.MetaQuery,
  },
]

export const TEMPLATE_VARIABLE_TYPES = {
  [TemplateType.CSV]: TemplateValueType.CSV,
  [TemplateType.Databases]: TemplateValueType.Database,
  [TemplateType.Measurements]: TemplateValueType.Measurement,
  [TemplateType.FieldKeys]: TemplateValueType.FieldKey,
  [TemplateType.TagKeys]: TemplateValueType.TagKey,
  [TemplateType.TagValues]: TemplateValueType.TagValue,
  [TemplateType.MetaQuery]: TemplateValueType.MetaQuery,
}

export const TEMPLATE_VARIABLE_QUERIES = {
  [TemplateType.Databases]: 'SHOW DATABASES',
  [TemplateType.Measurements]: 'SHOW MEASUREMENTS ON :database:',
  [TemplateType.FieldKeys]: 'SHOW FIELD KEYS ON :database: FROM :measurement:',
  [TemplateType.TagKeys]: 'SHOW TAG KEYS ON :database: FROM :measurement:',
  [TemplateType.TagValues]:
    'SHOW TAG VALUES ON :database: FROM :measurement: WITH KEY=:tagKey:',
}

interface DefaultTemplates {
  [templateType: string]: () => Template
}

export const DEFAULT_TEMPLATES: DefaultTemplates = {
  [BuilderType.Databases]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [
        {
          value: '_internal',
          type: TemplateValueType.Database,
          selected: true,
        },
      ],
      type: TemplateType.Databases,
      builderType: BuilderType.Databases,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.Databases],
      },
    }
  },
  [BuilderType.Measurements]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.Measurements,
      builderType: BuilderType.Measurements,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.Measurements],
        db: '',
      },
    }
  },
  [BuilderType.CSVManual]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      builderType: BuilderType.CSVManual,
      type: TemplateType.CSV,
      label: '',
      query: {},
    }
  },
  [BuilderType.TagKeys]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.TagKeys,
      builderType: BuilderType.TagKeys,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.TagKeys],
      },
    }
  },
  [BuilderType.FieldKeys]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.FieldKeys,
      builderType: BuilderType.FieldKeys,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.FieldKeys],
      },
    }
  },
  [BuilderType.TagValues]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.TagValues,
      builderType: BuilderType.TagValues,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.TagValues],
      },
    }
  },
  [BuilderType.MetaQuery]: () => {
    return {
      id: uuid.v4(),
      tempVar: ':my-meta-query:',
      values: [],
      type: TemplateType.MetaQuery,
      builderType: BuilderType.MetaQuery,
      label: '',
      query: {
        influxql: '',
      },
    }
  },
}

export const RESERVED_TEMPLATE_NAMES = [
  ':dashboardTime:',
  ':upperDashboardTime:',
  ':interval:',
  ':lower:',
  ':upper:',
  ':zoomedLower:',
  ':zoomedUpper:',
]

export const MATCH_INCOMPLETE_TEMPLATES = /:[\w-]*/g

export const applyMasks = query => {
  const matchWholeTemplates = /:([\w-]*):/g
  const maskForWholeTemplates = '😸$1😸'
  return query.replace(matchWholeTemplates, maskForWholeTemplates)
}
export const insertTempVar = (query, tempVar) => {
  return query.replace(MATCH_INCOMPLETE_TEMPLATES, tempVar)
}
export const unMask = query => {
  return query.replace(/😸/g, ':')
}
export const removeUnselectedTemplateValues = templates => {
  return templates.map(template => {
    const selectedValues = template.values.filter(value => value.selected)
    return {...template, values: selectedValues}
  })
}

export const TEMPLATE_RANGE: TimeRange = {
  upper: null,
  lower: TEMP_VAR_DASHBOARD_TIME,
}
