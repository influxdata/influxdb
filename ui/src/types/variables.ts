import {Variable as GenVariable} from 'src/client'
export {VariableProperties} from 'src/client'

import {
  VariableArgumentType,
  QueryArguments,
  MapArguments,
  CSVArguments,
  Label,
  RemoteDataState,
  NormalizedState,
} from 'src/types'

export interface Variable extends GenVariable {
  status: RemoteDataState // Loading status of an individual variable
  labels: Label[]
}

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

export interface VariablesState extends NormalizedState<Variable> {
  values: {
    // Different variable values can be selected in different
    // "contexts"---different parts of the app like a particular dashboard, or
    // the Data Explorer
    [contextID: string]: {
      status: RemoteDataState
      order: string[] // IDs of variables
      values: VariableValuesByID
    }
  }
}

export interface VariableEditorState {
  name: string
  selected: VariableArgumentType
  argsQuery: QueryArguments
  argsMap: MapArguments
  argsConstant: CSVArguments
}
