import {Source} from 'src/types/v2'
import {Action} from 'src/sources/actions'

export interface SourcesState {
  activeSourceID: string | null
  sources: {
    [sourceID: string]: Source
  }
}

const initialState: SourcesState = {
  activeSourceID: null,
  sources: {},
}

const sourcesReducer = (
  state: SourcesState = initialState,
  action: Action
): SourcesState => {
  switch (action.type) {
    case 'SET_ACTIVE_SOURCE': {
      const {activeSourceID} = action.payload

      return {...state, activeSourceID}
    }

    case 'SET_SOURCES': {
      const sources = {...state.sources}

      for (const source of action.payload.sources) {
        sources[source.id] = source
      }

      return {...state, sources}
    }

    case 'SET_SOURCE': {
      const {source} = action.payload

      return {...state, sources: {...state.sources, [source.id]: source}}
    }

    case 'REMOVE_SOURCE': {
      const sources = {...state.sources}

      delete sources[action.payload.sourceID]

      return {...state, sources}
    }
  }

  return state
}

export default sourcesReducer
