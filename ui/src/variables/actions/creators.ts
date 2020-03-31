// Types
import {
  VariableEntities,
  VariableArgumentType,
  RemoteDataState,
  QueryArguments,
  MapArguments,
  CSVArguments,
} from 'src/types'
import {NormalizedSchema} from 'normalizr'

export const SET_VARIABLES = 'SET_VARIABLES'
export const SET_VARIABLE = 'SET_VARIABLE'
export const REMOVE_VARIABLE = 'REMOVE_VARIABLE'
export const MOVE_VARIABLE = 'MOVE_VARIABLE'
export const SELECT_VARIABLE_VALUE = 'SELECT_VARIABLE_VALUE'

export type Action =
  | ReturnType<typeof setVariables>
  | ReturnType<typeof setVariable>
  | ReturnType<typeof removeVariable>
  | ReturnType<typeof moveVariable>
  | ReturnType<typeof selectValue>

// R is the type of the value of the "result" key in normalizr's normalization
type VariablesSchema<R extends string | string[]> = NormalizedSchema<
  VariableEntities,
  R
>

export const setVariables = (
  status: RemoteDataState,
  schema?: VariablesSchema<string[]>
) =>
  ({
    type: SET_VARIABLES,
    status,
    schema,
  } as const)

export const setVariable = (
  id: string,
  status: RemoteDataState,
  schema?: VariablesSchema<string>
) =>
  ({
    type: SET_VARIABLE,
    id,
    status,
    schema,
  } as const)

export const removeVariable = (id: string) =>
  ({
    type: REMOVE_VARIABLE,
    id,
  } as const)

export const moveVariable = (
  originalIndex: number,
  newIndex: number,
  contextID: string
) =>
  ({
    type: MOVE_VARIABLE,
    originalIndex,
    newIndex,
    contextID,
  } as const)

export const selectValue = (
  contextID: string,
  variableID: string,
  selectedValue: string
) =>
  ({
    type: SELECT_VARIABLE_VALUE,
    contextID,
    variableID,
    selectedValue,
  } as const)

// Variable Editor Action Creators
export type EditorAction =
  | ReturnType<typeof clearEditor>
  | ReturnType<typeof updateType>
  | ReturnType<typeof updateName>
  | ReturnType<typeof updateQuery>
  | ReturnType<typeof updateMap>
  | ReturnType<typeof updateConstant>

export const CLEAR_VARIABLE_EDITOR = 'CLEAR_VARIABLE_EDITOR'
export const CHANGE_VARIABLE_EDITOR_TYPE = 'CHANCE_VARIABLE_EDITOR_TYPE'
export const UPDATE_VARIABLE_EDITOR_NAME = 'UPDATE_VARIABLE_EDITOR_NAME'
export const UPDATE_VARIABLE_EDITOR_QUERY = 'UPDATE_VARIABLE_EDITOR_QUERY'
export const UPDATE_VARIABLE_EDITOR_MAP = 'UPDATE_VARIABLE_EDITOR_MAP'
export const UPDATE_VARIABLE_EDITOR_CONSTANT = 'UPDATE_VARIABLE_EDITOR_CONSTANT'

export const clearEditor = () =>
  ({
    type: CLEAR_VARIABLE_EDITOR,
  } as const)

export const updateType = (editorType: VariableArgumentType) =>
  ({
    type: CHANGE_VARIABLE_EDITOR_TYPE,
    editorType,
  } as const)

export const updateName = (name: string) =>
  ({
    type: UPDATE_VARIABLE_EDITOR_NAME,
    name,
  } as const)

export const updateQuery = (arg: QueryArguments) =>
  ({
    type: UPDATE_VARIABLE_EDITOR_QUERY,
    payload: arg,
  } as const)

export const updateMap = (arg: MapArguments) =>
  ({
    type: UPDATE_VARIABLE_EDITOR_MAP,
    payload: arg,
  } as const)

export const updateConstant = (arg: CSVArguments) =>
  ({
    type: UPDATE_VARIABLE_EDITOR_CONSTANT,
    payload: arg,
  } as const)
