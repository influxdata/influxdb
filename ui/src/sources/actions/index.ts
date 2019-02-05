// Libraries
import {Dispatch} from 'redux'

// APIs
import {client} from 'src/utils/api'

// Types
import {GetState} from 'src/types/v2'
import {Source} from 'src/api'

export type Action =
  | SetActiveSourceAction
  | SetSourcesAction
  | SetSourceAction
  | RemoveSourceAction

interface SetActiveSourceAction {
  type: 'SET_ACTIVE_SOURCE'
  payload: {
    activeSourceID: string | null
  }
}

export const setActiveSource = (
  activeSourceID: string | null
): SetActiveSourceAction => ({
  type: 'SET_ACTIVE_SOURCE',
  payload: {activeSourceID},
})

interface SetSourcesAction {
  type: 'SET_SOURCES'
  payload: {
    sources: Source[]
  }
}

export const setSources = (sources: Source[]): SetSourcesAction => ({
  type: 'SET_SOURCES',
  payload: {sources},
})

interface SetSourceAction {
  type: 'SET_SOURCE'
  payload: {
    source: Source
  }
}

export const setSource = (source: Source): SetSourceAction => ({
  type: 'SET_SOURCE',
  payload: {source},
})

interface RemoveSourceAction {
  type: 'REMOVE_SOURCE'
  payload: {
    sourceID: string
  }
}

export const removeSource = (sourceID: string): RemoveSourceAction => ({
  type: 'REMOVE_SOURCE',
  payload: {sourceID},
})

export const readSources = () => async (dispatch: Dispatch<Action>) => {
  const sources = await client.sources.getAll()

  dispatch(setSources(sources))
}

export const createSource = (attrs: Partial<Source>) => async (
  dispatch: Dispatch<Action>
) => {
  const source = await client.sources.create('', attrs)

  dispatch(setSource(source))
}

export const updateSource = (source: Source) => async (
  dispatch: Dispatch<Action>
) => {
  const updatedSource = await client.sources.update(source.id, source)

  dispatch(setSource(updatedSource))
}

export const deleteSource = (sourceID: string) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const source = getState().sources.sources[sourceID]

  if (!source) {
    throw new Error(`no source with ID "${sourceID}" exists`)
  }

  await client.sources.delete(source.id)

  dispatch(removeSource(sourceID))
}
