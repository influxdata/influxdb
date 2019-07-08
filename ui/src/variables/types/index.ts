export type FluxColumnType =
  | 'boolean'
  | 'unsignedLong'
  | 'long'
  | 'double'
  | 'string'
  | 'base64Binary'
  | 'dateTime'
  | 'duration'

export interface VariableValues {
  values: string[]
  valueType: FluxColumnType
  selectedValue: string
  error?: string
}

export interface VariableValuesByID {
  [variableID: string]: VariableValues
}

export interface ValueSelections {
  [variableID: string]: string
}
