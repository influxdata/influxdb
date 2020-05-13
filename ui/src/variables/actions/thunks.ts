// Libraries
import {normalize} from 'normalizr'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {setExportTemplate} from 'src/templates/actions/creators'
import {
  setVariables,
  setVariable,
  selectValue as selectValueInState,
  removeVariable,
  moveVariable as moveVariableInState,
} from 'src/variables/actions/creators'
import {updateQueryVars} from 'src/dashboards/actions/ranges'

// Schemas
import {variableSchema, arrayOfVariables} from 'src/schemas/variables'

// APIs
import * as api from 'src/client'
import {hydrateVars} from 'src/variables/utils/hydrateVars'
import {createVariableFromTemplate as createVariableFromTemplateAJAX} from 'src/templates/api'

// Utils
import {
  getVariable as getVariableFromState,
  getVariables as getVariablesFromState,
  getAllVariables as getAllVariablesFromState,
  normalizeValues,
} from 'src/variables/selectors'
import {variableToTemplate} from 'src/shared/utils/resourceToTemplate'
import {findDependentVariables} from 'src/variables/utils/exportVariables'
import {getOrg} from 'src/organizations/selectors'
import {getLabels, getStatus} from 'src/resources/selectors'
import {currentContext} from 'src/shared/selectors/currentContext'

// Constants
import * as copy from 'src/shared/copy/notifications'

// Types
import {Dispatch} from 'react'
import {
  GetState,
  RemoteDataState,
  VariableTemplate,
  Label,
  GenVariable,
  Variable,
  VariableEntities,
  ResourceType,
} from 'src/types'
import {Action as NotifyAction} from 'src/shared/actions/notifications'
import {
  Action as VariableAction,
  EditorAction,
} from 'src/variables/actions/creators'

type Action = VariableAction | EditorAction | NotifyAction

export const getVariables = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    const state = getState()

    if (
      getStatus(state, ResourceType.Variables) === RemoteDataState.NotStarted
    ) {
      dispatch(setVariables(RemoteDataState.Loading))
    }

    const org = getOrg(state)
    const resp = await api.getVariables({query: {orgID: org.id}})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const variables = normalize<Variable, VariableEntities, string[]>(
      resp.data.variables,
      arrayOfVariables
    )

    const varsByID = getVariablesFromState(state).reduce((prev, curr) => {
      prev[curr.id] = curr
      return prev
    }, {})

    // migrate the selected values from the existing variables into the new ones
    variables.result
      .map(k => {
        return variables.entities.variables[k]
      })
      .filter(e => {
        return varsByID.hasOwnProperty(e.id)
      })
      .forEach(v => {
        variables.entities.variables[v.id].selected = varsByID[v.id].selected
      })

    // Make sure all the queries are marked for update
    variables.result
      .map(k => {
        return variables.entities.variables[k]
      })
      .filter(e => {
        return e.arguments.type === 'query'
      })
      .forEach(v => {
        variables.entities.variables[v.id].status = RemoteDataState.NotStarted
      })

    await dispatch(setVariables(RemoteDataState.Done, variables))
  } catch (error) {
    console.error(error)
    dispatch(setVariables(RemoteDataState.Error))
    dispatch(notify(copy.getVariablesFailed()))
  }
}

// TODO: make this context aware
export const hydrateVariables = (skipCache?: boolean) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const state = getState()
  const org = getOrg(state)
  const vars = getVariablesFromState(state)
  const hydration = hydrateVars(vars, getAllVariablesFromState(state), {
    orgID: org.id,
    url: state.links.query.self,
    skipCache,
  })
  hydration.on('status', (variable, status) => {
    if (status === RemoteDataState.Loading) {
      dispatch(setVariable(variable.id, status))
      return
    }
    if (status === RemoteDataState.Done) {
      const _variable = normalize<Variable, VariableEntities, string>(
        variable,
        variableSchema
      )
      dispatch(setVariable(variable.id, RemoteDataState.Done, _variable))
    }
  })
  await hydration.promise
}

export const hydrateChangedVariable = (variableID: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const state = getState()
  const org = getOrg(state)
  const variable = getVariableFromState(state, variableID)
  const hydration = hydrateVars([variable], getAllVariablesFromState(state), {
    orgID: org.id,
    url: state.links.query.self,
    skipCache: true,
  })
  hydration.on('status', (variable, status) => {
    if (status === RemoteDataState.Loading) {
      dispatch(setVariable(variable.id, status))
      return
    }
    if (status === RemoteDataState.Done) {
      const _variable = normalize<Variable, VariableEntities, string>(
        variable,
        variableSchema
      )
      dispatch(setVariable(variable.id, RemoteDataState.Done, _variable))
    }
  })
  await hydration.promise
}

export const getVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const resp = await api.getVariable({variableID: id})
    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const variable = normalize<Variable, VariableEntities, string>(
      resp.data,
      variableSchema
    )

    dispatch(setVariable(id, RemoteDataState.Done, variable))
  } catch (error) {
    console.error(error)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(copy.getVariableFailed()))
  }
}

export const createVariable = (
  variable: Pick<GenVariable, 'name' | 'arguments'>
) => async (dispatch: Dispatch<Action>, getState: GetState) => {
  try {
    const org = getOrg(getState())
    const resp = await api.postVariable({
      data: {
        ...variable,
        orgID: org.id,
      },
    })

    if (resp.status !== 201) {
      throw new Error(resp.data.message)
    }

    const createdVar = normalize<Variable, VariableEntities, string>(
      resp.data,
      variableSchema
    )

    dispatch(setVariable(resp.data.id, RemoteDataState.Done, createdVar))
    dispatch(notify(copy.createVariableSuccess(variable.name)))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createVariableFailed(error.message)))
  }
}

export const createVariableFromTemplate = (
  template: VariableTemplate
) => async (dispatch: Dispatch<Action>, getState: GetState) => {
  try {
    const state = getState()
    const org = getOrg(state)
    const resp = await createVariableFromTemplateAJAX(template, org.id)

    const createdVar = normalize<Variable, VariableEntities, string>(
      resp,
      variableSchema
    )

    dispatch(setVariable(resp.id, RemoteDataState.Done, createdVar))
    dispatch(notify(copy.createVariableSuccess(resp.name)))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.createVariableFailed(error.message)))
  }
}

export const updateVariable = (id: string, props: Variable) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))

    const state = getState()
    const labels = getLabels(state, props.labels)

    const resp = await api.patchVariable({
      variableID: id,
      data: {
        ...(props as GenVariable),
        labels,
      },
    })

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const variable = normalize<Variable, VariableEntities, string>(
      resp.data,
      variableSchema
    )

    dispatch(setVariable(id, RemoteDataState.Done, variable))
    dispatch(notify(copy.updateVariableSuccess(resp.data.name)))
  } catch (error) {
    console.error(error)
    dispatch(setVariable(id, RemoteDataState.Error))
    dispatch(notify(copy.updateVariableFailed(error.message)))
  }
}

export const deleteVariable = (id: string) => async (
  dispatch: Dispatch<Action>
) => {
  try {
    dispatch(setVariable(id, RemoteDataState.Loading))
    const resp = await api.deleteVariable({variableID: id})
    if (resp.status !== 204) {
      throw new Error(resp.data.message)
    }
    dispatch(removeVariable(id))
    dispatch(notify(copy.deleteVariableSuccess()))
  } catch (error) {
    console.error(error)
    dispatch(setVariable(id, RemoteDataState.Done))
    dispatch(notify(copy.deleteVariableFailed(error.message)))
  }
}

export const moveVariable = (originalIndex: number, newIndex: number) => async (
  dispatch,
  getState: GetState
) => {
  const contextID = currentContext(getState())
  await dispatch(moveVariableInState(originalIndex, newIndex, contextID))
}

export const convertToTemplate = (variableID: string) => async (
  dispatch,
  getState: GetState
): Promise<void> => {
  try {
    dispatch(setExportTemplate(RemoteDataState.Loading))
    const state = getState()
    const org = getOrg(state)
    const resp = await api.getVariable({variableID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const allVariables = await api.getVariables({query: {orgID: org.id}})

    if (allVariables.status !== 200) {
      throw new Error(allVariables.data.message)
    }

    const normVariable = normalize<Variable, VariableEntities, string>(
      resp.data,
      variableSchema
    )

    const normVariables = normalize<Variable, VariableEntities, string>(
      allVariables.data.variables,
      arrayOfVariables
    )

    const variable = normVariable.entities.variables[normVariable.result]
    const variables = Object.values(normVariables.entities.variables)

    const dependencies = findDependentVariables(variable, variables)
    const variableTemplate = variableToTemplate(state, variable, dependencies)

    dispatch(setExportTemplate(RemoteDataState.Done, variableTemplate))
  } catch (error) {
    dispatch(setExportTemplate(RemoteDataState.Error))
    dispatch(notify(copy.createTemplateFailed(error)))
  }
}

export const addVariableLabelAsync = (
  variableID: string,
  label: Label
) => async (dispatch): Promise<void> => {
  try {
    const posted = await api.postVariablesLabel({
      variableID,
      data: {labelID: label.id},
    })

    if (posted.status !== 201) {
      throw new Error(posted.data.message)
    }

    const resp = await api.getVariable({variableID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const variable = normalize<Variable, VariableEntities, string>(
      resp.data,
      variableSchema
    )

    dispatch(setVariable(variableID, RemoteDataState.Done, variable))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.addVariableLabelFailed()))
  }
}

export const removeVariableLabelAsync = (
  variableID: string,
  label: Label
) => async (dispatch: Dispatch<Action>) => {
  try {
    const deleted = await api.deleteVariablesLabel({
      variableID,
      labelID: label.id,
    })

    if (deleted.status !== 204) {
      throw new Error(deleted.data.message)
    }

    const resp = await api.getVariable({variableID})

    if (resp.status !== 200) {
      throw new Error(resp.data.message)
    }

    const variable = normalize<Variable, VariableEntities, string>(
      resp.data,
      variableSchema
    )

    dispatch(setVariable(variableID, RemoteDataState.Done, variable))
  } catch (error) {
    console.error(error)
    dispatch(notify(copy.removeVariableLabelFailed()))
  }
}

export const selectValue = (variableID: string, selected: string) => async (
  dispatch: Dispatch<Action | ReturnType<typeof hydrateVariables>>,
  getState: GetState
) => {
  const state = getState()
  const contextID = currentContext(state)
  const variable = getVariableFromState(state, variableID)
  // Validate that we can make this selection
  const vals = normalizeValues(variable)
  if (!vals.includes(selected)) {
    return
  }

  await dispatch(selectValueInState(contextID, variableID, selected))
  // only hydrate the changedVariable
  dispatch(hydrateChangedVariable(variableID))
  // dispatch(hydrateVariables(true))
  dispatch(updateQueryVars({[variable.name]: selected}))
}
