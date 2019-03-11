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
  createVariableSuccess,
} from 'src/shared/copy/notifications'

// Utils
import {getValueSelections, getVariablesForOrg} from 'src/variables/selectors'

// Types
import {Dispatch} from 'redux-thunk'
import {RemoteDataState} from 'src/types'
import {GetState} from 'src/types/v2'
import {Variable} from '@influxdata/influx'
import {VariableValuesByID} from 'src/variables/types'

export type Action = SetVariables | SetVariable | RemoveVariable | SetValues

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

export const getVariables = () => async (dispatch: Dispatch<Action>) => {
  try {
    dispatch(setVariables(RemoteDataState.Loading))

    const variables = await client.variables.getAll()

    dispatch(setVariables(RemoteDataState.Done, variables))
  } catch (e) {
    console.log(e)
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
    console.log(e)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(getVariableFailed()))
  }
}

export const createVariable = (variable: Variable) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    const createdVariable = await client.variables.create(variable)

    dispatch(
      setVariable(createdVariable.id, RemoteDataState.Done, createdVariable)
    )
    dispatch(notify(createVariableSuccess(variable.name)))
  } catch (e) {
    console.log(e)
    dispatch(notify(createVariableFailed()))
  }
}

export const updateVariable = (id: string, props: Partial<Variable>) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const variable = await client.variables.update(id, props)

    dispatch(setVariable(id, RemoteDataState.Done, variable))
  } catch (e) {
    console.log(e)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(updateVariableFailed()))
  }
}

export const deleteVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    await client.variables.delete(id)

    dispatch(removeVariable(id))
  } catch (e) {
    console.log(e)
    dispatch(setVariable(id, RemoteDataState.Done))
    dispatch(notify(deleteVariableFailed()))
  }
}

export const refreshVariableValues = (
  contextID: string,
  orgID: string,
  variables: Variable[]
) => async (dispatch: Dispatch<Action>, getState: GetState): Promise<void> => {
  dispatch(setValues(contextID, RemoteDataState.Loading))

  try {
    const url = getState().links.query.self
    const selections = getValueSelections(getState(), contextID)
    const allVariables = getVariablesForOrg(getState(), orgID)

    const values = await hydrateVars(variables, allVariables, {
      url,
      orgID,
      selections,
    }).promise

    dispatch(setValues(contextID, RemoteDataState.Done, values))
  } catch (e) {
    console.error(e)
    dispatch(setValues(contextID, RemoteDataState.Error))
  }
}
