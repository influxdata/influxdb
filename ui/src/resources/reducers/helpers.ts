// Libraries
import {get} from 'lodash'

// Types
import {ResourceType, NormalizedState} from 'src/types'

export const setResource = <R>(
  draftState: NormalizedState<R>,
  action,
  resource: ResourceType
) => {
  const {status, schema} = action

  draftState.status = status
  if (!draftState.byID) {
    draftState.byID = {}
  }

  if (get(schema, 'entities')) {
    draftState.byID = schema.entities[resource]
    draftState.allIDs = schema.result
  }

  return
}

export const addResource = <R>(
  draftState: NormalizedState<R>,
  action,
  resource: ResourceType
) => {
  const {result, entities} = action.schema
  const r = entities[resource][result]

  if (!draftState.byID) {
    draftState.byID = {}
  }

  draftState.byID[result] = r
  draftState.allIDs.push(result)
}

export const editResource = <R>(
  draftState: NormalizedState<R>,
  action,
  resource: ResourceType
) => {
  const {entities, result} = action.schema
  if (!draftState.byID) {
    draftState.byID = {}
  }

  const bucket = entities[resource][result]
  draftState.byID[result] = bucket
}

interface RemoveAction {
  type: string
  id: string
}

export const removeResource = <R>(
  draftState: NormalizedState<R>,
  action: RemoveAction
) => {
  const {id} = action
  delete draftState.byID[id]
  draftState.allIDs = draftState.allIDs.filter(uuid => uuid !== id)

  return
}
