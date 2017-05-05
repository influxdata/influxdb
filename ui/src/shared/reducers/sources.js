import _ from 'lodash'

const getInitialState = () => []

const initialState = getInitialState()

const sourcesReducer = (state = initialState, action) => {
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

    case 'LOAD_KAPACITORS': {
      const {source, kapacitors} = action.payload
      const sourceIndex = state.findIndex(s => s.id === source.id)
      const updatedSources = _.cloneDeep(state)
      if (updatedSources[sourceIndex]) {
        updatedSources[sourceIndex].kapacitors = kapacitors
      }
      return updatedSources
    }

    case 'SET_ACTIVE_KAPACITOR': {
      const {kapacitor} = action.payload
      const updatedSources = _.cloneDeep(state)
      updatedSources.forEach(source => {
        source.kapacitors.forEach((k, i) => {
          source.kapacitors[i].active = k.id === kapacitor.id
        })
      })
      return updatedSources
    }

    case 'DELETE_KAPACITOR': {
      const {kapacitor} = action.payload
      const updatedSources = _.cloneDeep(state)
      updatedSources.forEach(source => {
        const index = _.findIndex(source.kapacitors, k => k.id === kapacitor.id)
        if (index >= 0) {
          source.kapacitors.splice(index, 1)
        }
      })
      return updatedSources
    }
  }

  return state
}

export default sourcesReducer
