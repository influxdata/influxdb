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
import {WrappedCancelablePromise, CancellationError} from 'src/types/promises'
import {variableToTemplate} from 'src/shared/utils/resourceToTemplate'
import {findDepedentVariables} from 'src/variables/utils/exportVariables'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {Dispatch} from 'redux-thunk'
import {RemoteDataState, VariableTemplate} from 'src/types'
import {GetState} from 'src/types'
import {IVariable as Variable, ILabel as Label} from '@influxdata/influx'
import {VariableValuesByID} from 'src/variables/types'
import {
  addVariableLabelFailed,
  removeVariableLabelFailed,
} from 'src/shared/copy/notifications'

export type Action =
  | SetVariables
  | SetVariable
  | RemoveVariable
  | MoveVariable
  | SetValues
  | SelectValue

interface SetVariables {
  type: 'SET_VARIABLES'
  payload: {
    status: RemoteDataState
    variables?: Variable[]
  }
}

const setVariables = (
  status: RemoteDataState,
  variables?: Variable[]
): SetVariables => ({
  type: 'SET_VARIABLES',
  payload: {status, variables},
})

interface SetVariable {
  type: 'SET_VARIABLE'
  payload: {
    id: string
    status: RemoteDataState
    variable?: Variable
  }
}

const setVariable = (
  id: string,
  status: RemoteDataState,
  variable?: Variable
): SetVariable => ({
  type: 'SET_VARIABLE',
  payload: {id, status, variable},
})

interface RemoveVariable {
  type: 'REMOVE_VARIABLE'
  payload: {id: string}
}

const removeVariable = (id: string): RemoveVariable => ({
  type: 'REMOVE_VARIABLE',
  payload: {id},
})

interface MoveVariable {
  type: 'MOVE_VARIABLE'
  payload: {originalIndex: number; newIndex: number; contextID: string}
}

export const moveVariable = (
  originalIndex: number,
  newIndex: number,
  contextID: string
): MoveVariable => ({
  type: 'MOVE_VARIABLE',
  payload: {originalIndex, newIndex, contextID},
})

interface SetValues {
  type: 'SET_VARIABLE_VALUES'
  payload: {
    contextID: string
    status: RemoteDataState
    values?: VariableValuesByID
  }
}

const setValues = (
  contextID: string,
  status: RemoteDataState,
  values?: VariableValuesByID
): SetValues => ({
  type: 'SET_VARIABLE_VALUES',
  payload: {contextID, status, values},
})

interface SelectValue {
  type: 'SELECT_VARIABLE_VALUE'
  payload: {
    contextID: string
    variableID: string
    selectedValue: string
  }
}

export const selectValue = (
  contextID: string,
  variableID: string,
  selectedValue: string
): SelectValue => ({
  type: 'SELECT_VARIABLE_VALUE',
  payload: {contextID, variableID, selectedValue},
})

export const getVariables = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    dispatch(setVariables(RemoteDataState.Loading))
    const {
      orgs: {org},
    } = getState()
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
    const {
      orgs: {org},
    } = getState()
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
    const {
      orgs: {org},
    } = getState()
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
  [contextID: string]: WrappedCancelablePromise<VariableValuesByID>
}

let pendingValueRequests: PendingValueRequests = {}

export const refreshVariableValues = (
  contextID: string,
  variables: Variable[]
) => async (dispatch: Dispatch<Action>, getState: GetState): Promise<void> => {
  dispatch(setValues(contextID, RemoteDataState.Loading))

  try {
    const {
      orgs: {org},
    } = getState()
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
    if (e instanceof CancellationError) {
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
    const {
      orgs: {org},
    } = getState()
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
