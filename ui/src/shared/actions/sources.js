import {deleteSource,
  getSources,
  getKapacitors as getKapacitorsAJAX,
} from 'src/shared/apis'
import {publishNotification} from './notifications'

export const loadSources = (sources) => ({
  type: 'LOAD_SOURCES',
  payload: {
    sources,
  },
})

export const updateSource = (source) => ({
  type: 'SOURCE_UPDATED',
  payload: {
    source,
  },
})

export const addSource = (source) => ({
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

// Async action creators

export const removeAndLoadSources = (source) => async (dispatch) => {
  try {
    try {
      await deleteSource(source)
    } catch (err) {
      // A 404 means that either a concurrent write occurred or the source
      // passed to this action creator doesn't exist (or is undefined)
      if (err.status !== 404) { // eslint-disable-line no-magic-numbers
        throw (err)
      }
    }

    const {data: {sources: newSources}} = await getSources()
    dispatch(loadSources(newSources))
  } catch (err) {
    dispatch(publishNotification("error", "Internal Server Error. Check API Logs"))
  }
}

export const fetchKapacitorsAsync = (source) => async (dispatch) => {
  try {
    const {data} = await getKapacitorsAJAX(source)
    dispatch(fetchKapacitors(source, data.kapacitors))
  } catch (err) {
    dispatch(publishNotification('error', `Internal Server Error. Could not retrieve kapacitors for source ${source.id}.`))
  }
}
