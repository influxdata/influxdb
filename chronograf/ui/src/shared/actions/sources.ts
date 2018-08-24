import {deleteSource, getSources as getSourcesAJAX} from 'src/sources/apis/v2'

import {notify} from './notifications'
import {errorThrown} from 'src/shared/actions/errors'

import {HTTP_NOT_FOUND} from 'src/shared/constants'
import {serverError} from 'src/shared/copy/notifications'

import {Source} from 'src/types/v2'

export type Action = ActionLoadSources | ActionUpdateSource | ActionAddSource

// Load Sources
export type LoadSources = (sources: Source[]) => ActionLoadSources
export interface ActionLoadSources {
  type: 'LOAD_SOURCES'
  payload: {
    sources: Source[]
  }
}

export const loadSources = (sources: Source[]): ActionLoadSources => ({
  type: 'LOAD_SOURCES',
  payload: {
    sources,
  },
})

export type UpdateSource = (source: Source) => ActionUpdateSource
export interface ActionUpdateSource {
  type: 'SOURCE_UPDATED'
  payload: {
    source: Source
  }
}

export const updateSource = (source: Source): ActionUpdateSource => ({
  type: 'SOURCE_UPDATED',
  payload: {
    source,
  },
})

export type AddSource = (source: Source) => ActionAddSource
export interface ActionAddSource {
  type: 'SOURCE_ADDED'
  payload: {
    source: Source
  }
}

export const addSource = (source: Source): ActionAddSource => ({
  type: 'SOURCE_ADDED',
  payload: {
    source,
  },
})

export type RemoveAndLoadSources = (
  source: Source
) => (dispatch) => Promise<void>
// Async action creators
export const removeAndLoadSources = (source: Source) => async (
  dispatch
): Promise<void> => {
  try {
    try {
      await deleteSource(source)
    } catch (err) {
      // A 404 means that either a concurrent write occurred or the source
      // passed to this action creator doesn't exist (or is undefined)
      if (err.status !== HTTP_NOT_FOUND) {
        // eslint-disable-line no-magic-numbers
        throw err
      }
    }

    const newSources = await getSourcesAJAX()
    dispatch(loadSources(newSources))
  } catch (err) {
    dispatch(notify(serverError))
  }
}

export const getSourcesAsync = () => async (dispatch): Promise<void> => {
  try {
    const sources = await getSourcesAJAX()
    dispatch(loadSources(sources))
  } catch (error) {
    dispatch(errorThrown(error))
  }
}
