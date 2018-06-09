export interface TemplateValue {
  value: string
  type: string
  selected: boolean
}

export interface TemplateQuery {
  command: string
  db: string
  database?: string
  rp?: string
  measurement: string
  tagKey: string
  fieldKey: string
  influxql: string
}

export interface Template {
  id: string
  tempVar: string
  values: TemplateValue[]
  type: string
  label: string
  query?: TemplateQuery
}

export interface URLQueries {
  [key: string]: string
}
