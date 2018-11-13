// Libraries
import {Dispatch} from 'redux'

// APIs
import {
  readSources as readSourcesAJAX,
  createSource as createSourceAJAX,
  updateSource as updateSourceAJAX,
  deleteSource as deleteSourceAJAX,
} from 'src/sources/apis'

// Types
import {Source, GetState} from 'src/types/v2'

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

export const readSources = () => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const sourcesLink = getState().links.sources
  const sources = await readSourcesAJAX(sourcesLink)

  dispatch(setSources(sources))
}

export const createSource = (attrs: Partial<Source>) => async (
  dispatch: Dispatch<Action>,
  getState: GetState
) => {
  const sourcesLink = getState().links.sources
  const source = await createSourceAJAX(sourcesLink, attrs)

  dispatch(setSource(source))
}

export const updateSource = (source: Source) => async (
  dispatch: Dispatch<Action>
) => {
  const updatedSource = await updateSourceAJAX(source)

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

  await deleteSourceAJAX(source)

  dispatch(removeSource(sourceID))
}
