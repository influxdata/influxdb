// API
import {client} from 'src/utils/api'
import {hydrateVars} from 'src/variables/utils/hydrateVars'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {
  getVariablesFailed,
  getVariableFailed,
  createVariableFailed,
  updateVariableFailed,
  deleteVariableFailed,
  deleteVariableSuccess,
  createVariableSuccess,
  updateVariableSuccess,
} from 'src/shared/copy/notifications'
import {setExportTemplate} from 'src/templates/actions'

// APIs
import {createVariableFromTemplate as createVariableFromTemplateAJAX} from 'src/templates/api'

// Utils
import {getValueSelections, extractVariablesList} from 'src/variables/selectors'
import {CancelBox} from 'src/types/promises'
import {variableToTemplate} from 'src/shared/utils/resourceToTemplate'
import {findDepedentVariables} from 'src/variables/utils/exportVariables'
import {getOrg} from 'src/organizations/selectors'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {Dispatch} from 'react'
import {
  GetState,
  VariableArgumentType,
  RemoteDataState,
  VariableTemplate,
  QueryArguments,
  MapArguments,
  CSVArguments,
  Label,
} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'
import {VariableValuesByID} from 'src/variables/types'
import {
  addVariableLabelFailed,
  removeVariableLabelFailed,
} from 'src/shared/copy/notifications'
import {Action as NotifyAction} from 'src/shared/actions/notifications'

export type EditorAction =
  | ReturnType<typeof clearEditor>
  | ReturnType<typeof updateType>
  | ReturnType<typeof updateName>
  | ReturnType<typeof updateQuery>
  | ReturnType<typeof updateMap>
  | ReturnType<typeof updateConstant>

export const clearEditor = () => ({
  type: 'CLEAR_VARIABLE_EDITOR' as 'CLEAR_VARIABLE_EDITOR',
})

export const updateType = (type: VariableArgumentType) => ({
  type: 'CHANGE_VARIABLE_EDITOR_TYPE' as 'CHANGE_VARIABLE_EDITOR_TYPE',
  payload: type,
})

export const updateName = (name: string) => ({
  type: 'UPDATE_VARIABLE_EDITOR_NAME' as 'UPDATE_VARIABLE_EDITOR_NAME',
  payload: name,
})

export const updateQuery = (arg: QueryArguments) => ({
  type: 'UPDATE_VARIABLE_EDITOR_QUERY' as 'UPDATE_VARIABLE_EDITOR_QUERY',
  payload: arg,
})

export const updateMap = (arg: MapArguments) => ({
  type: 'UPDATE_VARIABLE_EDITOR_MAP' as 'UPDATE_VARIABLE_EDITOR_MAP',
  payload: arg,
})

export const updateConstant = (arg: CSVArguments) => ({
  type: 'UPDATE_VARIABLE_EDITOR_CONSTANT' as 'UPDATE_VARIABLE_EDITOR_CONSTANT',
  payload: arg,
})

export type Action =
  | ReturnType<typeof setVariables>
  | ReturnType<typeof setVariable>
  | ReturnType<typeof removeVariable>
  | ReturnType<typeof moveVariable>
  | ReturnType<typeof setValues>
  | ReturnType<typeof selectValue>
  | NotifyAction

const setVariables = (status: RemoteDataState, variables?: Variable[]) => ({
  type: 'SET_VARIABLES' as 'SET_VARIABLES',
  payload: {status, variables},
})

const setVariable = (
  id: string,
  status: RemoteDataState,
  variable?: Variable
) => ({
  type: 'SET_VARIABLE' as 'SET_VARIABLE',
  payload: {id, status, variable},
})

const removeVariable = (id: string) => ({
  type: 'REMOVE_VARIABLE' as 'REMOVE_VARIABLE',
  payload: {id},
})

export const moveVariable = (
  originalIndex: number,
  newIndex: number,
  contextID: string
) => ({
  type: 'MOVE_VARIABLE' as 'MOVE_VARIABLE',
  payload: {originalIndex, newIndex, contextID},
})

export const setValues = (
  contextID: string,
  status: RemoteDataState,
  values?: VariableValuesByID
) => ({
  type: 'SET_VARIABLE_VALUES' as 'SET_VARIABLE_VALUES',
  payload: {contextID, status, values},
})

export const selectValue = (
  contextID: string,
  variableID: string,
  selectedValue: string
) => ({
  type: 'SELECT_VARIABLE_VALUE' as 'SELECT_VARIABLE_VALUE',
  payload: {contextID, variableID, selectedValue},
})

export const getVariables = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    dispatch(setVariables(RemoteDataState.Loading))
    const org = getOrg(getState())
    const variables = await client.variables.getAll(org.id)

    dispatch(setVariables(RemoteDataState.Done, variables))
  } catch (e) {
    console.error(e)
    dispatch(setVariables(RemoteDataState.Error))
    dispatch(notify(getVariablesFailed()))
  }
}

export const getVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const variable = await client.variables.get(id)

    dispatch(setVariable(id, RemoteDataState.Done, variable))
  } catch (e) {
    console.error(e)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(getVariableFailed()))
  }
}

export const createVariable = (
  variable: Pick<Variable, 'name' | 'arguments'>
) => async (dispatch: Dispatch<Action>, getState: GetState) => {
  try {
    const org = getOrg(getState())
    const createdVariable = await client.variables.create({
      ...variable,
      orgID: org.id,
    })

    dispatch(
      setVariable(createdVariable.id, RemoteDataState.Done, createdVariable)
    )
    dispatch(notify(createVariableSuccess(variable.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(createVariableFailed(e.response.data.message)))
  }
}

export const createVariableFromTemplate = (
  template: VariableTemplate
) => async (dispatch: Dispatch<Action>, getState: GetState) => {
  try {
    const org = getOrg(getState())
    const createdVariable = await createVariableFromTemplateAJAX(
      template,
      org.id
    )

    dispatch(
      setVariable(createdVariable.id, RemoteDataState.Done, createdVariable)
    )
    dispatch(notify(createVariableSuccess(createdVariable.name)))
  } catch (e) {
    console.error(e)
    dispatch(notify(createVariableFailed(e.response.data.message)))
  }
}

export const updateVariable = (id: string, props: Partial<Variable>) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const variable = await client.variables.update(id, props)

    dispatch(setVariable(id, RemoteDataState.Done, variable))
    dispatch(notify(updateVariableSuccess(variable.name)))
  } catch (e) {
    console.error(e)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(updateVariableFailed(e.response.data.message)))
  }
}

export const deleteVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))
    await client.variables.delete(id)
    dispatch(removeVariable(id))
    dispatch(notify(deleteVariableSuccess()))
  } catch (e) {
    console.error(e)
    dispatch(setVariable(id, RemoteDataState.Done))
    dispatch(notify(deleteVariableFailed(e.response.data.message)))
  }
}

interface PendingValueRequests {
  [contextID: string]: CancelBox<VariableValuesByID>
}

const pendingValueRequests: PendingValueRequests = {}

export const refreshVariableValues = (
  contextID: string,
  variables: Variable[]
) => async (dispatch: Dispatch<Action>, getState: GetState): Promise<void> => {
  dispatch(setValues(contextID, RemoteDataState.Loading))

  try {
    const org = getOrg(getState())
    const url = getState().links.query.self
    const selections = getValueSelections(getState(), contextID)
    const allVariables = extractVariablesList(getState())

    if (pendingValueRequests[contextID]) {
      pendingValueRequests[contextID].cancel()
    }

    pendingValueRequests[contextID] = hydrateVars(variables, allVariables, {
      url,
      orgID: org.id,
      selections,
    })

    const values = await pendingValueRequests[contextID].promise

    dispatch(setValues(contextID, RemoteDataState.Done, values))
  } catch (e) {
    if (e.name === 'CancellationError') {
      return
    }

    console.error(e)
    dispatch(setValues(contextID, RemoteDataState.Error))
  }
}

export const convertToTemplate = (variableID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))
    const org = getOrg(getState())
    const variable = await client.variables.get(variableID)
    const allVariables = await client.variables.getAll(org.id)

    const dependencies = findDepedentVariables(variable, allVariables)
    const variableTemplate = variableToTemplate(variable, dependencies)

    dispatch(setExportTemplate(RemoteDataState.Done, variableTemplate))
  } catch (error) {
    dispatch(setExportTemplate(RemoteDataState.Error))
    dispatch(notify(copy.createTemplateFailed(error)))
  }
}

export const addVariableLabelsAsync = (
  variableID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.variables.addLabels(variableID, labels.map(l => l.id))
    const variable = await client.variables.get(variableID)

    dispatch(setVariable(variableID, RemoteDataState.Done, variable))
  } catch (error) {
    console.error(error)
    dispatch(notify(addVariableLabelFailed()))
  }
}

export const removeVariableLabelsAsync = (
  variableID: string,
  labels: Label[]
) => async (dispatch): Promise<void> => {
  try {
    await client.variables.removeLabels(variableID, labels.map(l => l.id))
    const variable = await client.variables.get(variableID)

    dispatch(setVariable(variableID, RemoteDataState.Done, variable))
  } catch (error) {
    console.error(error)
    dispatch(notify(removeVariableLabelFailed()))
  }
}
