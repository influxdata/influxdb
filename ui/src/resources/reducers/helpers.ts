// Libraries
import {get} from 'lodash'

// Types
import {ResourceType, NormalizedState, RemoteDataState} from 'src/types'

export const setResourceAtID = <R extends {status: RemoteDataState}>(
  draftState: NormalizedState<R>,
  action,
  resource: ResourceType
) => {
  const {schema, status, id} = action

  const prevResource = get(draftState, ['byID', id], {})
  const currentResource = get(schema, ['entities', resource, id], {})

  if (!draftState.allIDs.includes(id)) {
    draftState.allIDs.push(id)
  }

  draftState.byID[id] = {...prevResource, ...currentResource, status}
}

export const setResource = <R>(
  draftState: NormalizedState<R>,
  action,
  resource: ResourceType
) => {
  const {status, schema} = action

  draftState.status = status

  if (get(schema, ['entities', resource])) {
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

  draftState.byID[result] = entities[resource][result]
  draftState.allIDs.push(result)
}

export const editResource = <R>(
  draftState: NormalizedState<R>,
  action,
  resource: ResourceType
) => {
  const {entities, result} = action.schema

  draftState.byID[result] = entities[resource][result]
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

export const setRelation = <R>(
  draftState: NormalizedState<R>,
  childType: ResourceType,
  childID: string,
  parentID: string
) => {
  const resource = draftState.byID[parentID]

  if (!resource) {
    return
  }

  const child = draftState.byID[parentID][childType]

  if (!child) {
    return
  }

  draftState.byID[parentID][childType].push(childID)
}
