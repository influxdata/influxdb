export interface Tag {
  [tagName: string]: string[]
}

export interface SchemaValues {
  fields: string[]
  tags: Tag
  type?: string
}

export type Measurement = string

export interface Schema {
  [measurement: string]: SchemaValues
}
