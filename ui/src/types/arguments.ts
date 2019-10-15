interface Argument<T> {
  type: VariableArgumentType
  values: T
}

export interface MapArguments extends Argument<KeyValueMap> {
  type: 'map'
}

export interface QueryArguments extends Argument<QueryValue> {
  type: 'query'
}

export interface CSVArguments extends Argument<string[]> {
  type: 'constant'
}

export type KeyValueMap = {[key: string]: string}
export type QueryValue = {language: 'flux'; query: string}

export type VariableArguments = QueryArguments | MapArguments | CSVArguments
export type VariableArgumentType = 'query' | 'map' | 'constant'
