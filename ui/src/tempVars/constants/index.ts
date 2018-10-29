import uuid from 'uuid'

import {TimeRange} from 'src/types/queries'
import {TEMP_VAR_DASHBOARD_TIME} from 'src/shared/constants'
import {Template, TemplateType, TemplateValueType} from 'src/types'

interface TemplateTypesListItem {
  text: string
  type: TemplateType
}

export const TEMPLATE_TYPES_LIST: TemplateTypesListItem[] = [
  {
    text: 'Databases',
    type: TemplateType.Databases,
  },
  {
    text: 'Measurements',
    type: TemplateType.Measurements,
  },
  {
    text: 'Field Keys',
    type: TemplateType.FieldKeys,
  },
  {
    text: 'Tag Keys',
    type: TemplateType.TagKeys,
  },
  {
    text: 'Tag Values',
    type: TemplateType.TagValues,
  },
  {
    text: 'CSV',
    type: TemplateType.CSV,
  },
  {
    text: 'Map',
    type: TemplateType.Map,
  },
  {
    text: 'Custom Meta Query',
    type: TemplateType.MetaQuery,
  },
  {
    text: 'Text',
    type: TemplateType.Text,
  },
]

export const TEMPLATE_VARIABLE_TYPES = {
  [TemplateType.CSV]: TemplateValueType.CSV,
  [TemplateType.Map]: TemplateValueType.Map,
  [TemplateType.Databases]: TemplateValueType.Database,
  [TemplateType.Measurements]: TemplateValueType.Measurement,
  [TemplateType.FieldKeys]: TemplateValueType.FieldKey,
  [TemplateType.TagKeys]: TemplateValueType.TagKey,
  [TemplateType.TagValues]: TemplateValueType.TagValue,
  [TemplateType.MetaQuery]: TemplateValueType.MetaQuery,
  [TemplateType.Text]: TemplateValueType.Constant,
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
  [TemplateType.Databases]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [
        {
          value: '_internal',
          type: TemplateValueType.Database,
          selected: true,
          localSelected: true,
        },
      ],
      type: TemplateType.Databases,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.Databases],
      },
    }
  },
  [TemplateType.Measurements]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.Measurements,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.Measurements],
        db: '',
      },
    }
  },
  [TemplateType.CSV]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.CSV,
      label: '',
      query: {},
    }
  },
  [TemplateType.Map]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.Map,
      label: '',
      query: {},
    }
  },
  [TemplateType.TagKeys]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.TagKeys,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.TagKeys],
      },
    }
  },
  [TemplateType.FieldKeys]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.FieldKeys,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.FieldKeys],
      },
    }
  },
  [TemplateType.TagValues]: () => {
    return {
      id: uuid.v4(),
      tempVar: '',
      values: [],
      type: TemplateType.TagValues,
      label: '',
      query: {
        influxql: TEMPLATE_VARIABLE_QUERIES[TemplateType.TagValues],
      },
    }
  },
  [TemplateType.MetaQuery]: () => {
    return {
      id: uuid.v4(),
      tempVar: ':my-meta-query:',
      values: [],
      type: TemplateType.MetaQuery,
      label: '',
      query: {
        influxql: '',
      },
    }
  },
  [TemplateType.Text]: () => {
    return {
      id: uuid.v4(),
      tempVar: ':my-text:',
      values: [],
      type: TemplateType.Text,
      label: '',
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
export const TEMPLATE_RANGE: TimeRange = {
  upper: null,
  lower: TEMP_VAR_DASHBOARD_TIME,
}

interface PreviewListDimensions {
  [index: string]: number
}

export const TEMPLATE_PREVIEW_LIST_DIMENSIONS: Readonly<
  PreviewListDimensions
> = {
  RESULTS_TO_DISPLAY: 9,
  LI_HEIGHT: 28,
  LI_MARGIN_BOTTOM: 2,
  OFFSET: 14,
}
