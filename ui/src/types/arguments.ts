export interface MapArguments {
  type: 'map'
  values: KeyValueMap
}

export type KeyValueMap = {[key: string]: string}

export interface QueryArguments {
  type: 'query'
  values: {
    language: 'flux'
    query: string
  }
}

export interface CSVArguments {
  type: 'constant'
  values: string[]
}

export type VariableArguments = QueryArguments | MapArguments | CSVArguments
