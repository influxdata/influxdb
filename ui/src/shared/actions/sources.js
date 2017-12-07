import {
  deleteSource,
  getSources as getSourcesAJAX,
  getKapacitors as getKapacitorsAJAX,
  updateKapacitor as updateKapacitorAJAX,
  deleteKapacitor as deleteKapacitorAJAX,
} from 'shared/apis'
import {publishNotification} from './notifications'
import {errorThrown} from 'shared/actions/errors'

import {HTTP_NOT_FOUND} from 'shared/constants'

export const loadSources = sources => ({
  type: 'LOAD_SOURCES',
  payload: {
    sources,
  },
})

export const updateSource = source => ({
  type: 'SOURCE_UPDATED',
  payload: {
    source,
  },
})

export const addSource = source => ({
  type: 'SOURCE_ADDED',
  payload: {
    source,
  },
})

export const fetchKapacitors = (source, kapacitors) => ({
  type: 'LOAD_KAPACITORS',
  payload: {
    source,
    kapacitors,
  },
})

export const setActiveKapacitor = kapacitor => ({
  type: 'SET_ACTIVE_KAPACITOR',
  payload: {
    kapacitor,
  },
})

export const deleteKapacitor = kapacitor => ({
  type: 'DELETE_KAPACITOR',
  payload: {
    kapacitor,
  },
})

// Async action creators

export const removeAndLoadSources = source => async dispatch => {
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

    const {data: {sources: newSources}} = await getSourcesAJAX()
    dispatch(loadSources(newSources))
  } catch (err) {
    dispatch(
      publishNotification('error', 'Internal Server Error. Check API Logs')
    )
  }
}

export const fetchKapacitorsAsync = source => async dispatch => {
  try {
    const {data} = await getKapacitorsAJAX(source)
    dispatch(fetchKapacitors(source, data.kapacitors))
  } catch (err) {
    dispatch(
      publishNotification(
        'error',
        `Internal Server Error. Could not retrieve kapacitors for source ${source.id}.`
      )
    )
  }
}

export const setActiveKapacitorAsync = kapacitor => async dispatch => {
  // eagerly update the redux state
  dispatch(setActiveKapacitor(kapacitor))
  const kapacitorPost = {...kapacitor, active: true}
  await updateKapacitorAJAX(kapacitorPost)
}

export const deleteKapacitorAsync = kapacitor => async dispatch => {
  try {
    await deleteKapacitorAJAX(kapacitor)
    dispatch(deleteKapacitor(kapacitor))
  } catch (err) {
    dispatch(
      publishNotification(
        'error',
        'Internal Server Error. Could not delete Kapacitor config.'
      )
    )
  }
}

export const getSourcesAsync = () => async dispatch => {
  try {
    const {data: {sources}} = await getSourcesAJAX()
    dispatch(loadSources(sources))
    return sources
  } catch (error) {
    dispatch(errorThrown(error))
  }
}
