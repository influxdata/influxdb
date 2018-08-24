import {Source} from 'src/types'
import {Action} from 'src/shared/actions/sources'

export const initialState: Source[] = []

const sourcesReducer = (state = initialState, action: Action): Source[] => {
  switch (action.type) {
    case 'LOAD_SOURCES': {
      return action.payload.sources
    }

    case 'SOURCE_UPDATED': {
      const {source} = action.payload
      const updatedIndex = state.findIndex(s => s.id === source.id)
      const updatedSources = source.default
        ? state.map(s => {
            s.default = false
            return s
          })
        : [...state]
      updatedSources[updatedIndex] = source
      return updatedSources
    }

    case 'SOURCE_ADDED': {
      const {source} = action.payload
      const updatedSources = source.default
        ? state.map(s => {
            s.default = false
            return s
          })
        : state
      return [...updatedSources, source]
    }
  }

  return state
}

export default sourcesReducer
