import {Action} from 'src/shared/actions/services'
import {Service} from 'src/types'

export const initialState: Service[] = []

const servicesReducer = (state = initialState, action: Action): Service[] => {
  switch (action.type) {
    case 'LOAD_SERVICES': {
      return action.payload.services
    }

    case 'ADD_SERVICE': {
      const {service} = action.payload
      return [...state, service]
    }

    case 'DELETE_SERVICE': {
      const {service} = action.payload
      const services = state.filter(s => s.id !== service.id)
      return services
    }

    case 'UPDATE_SERVICE': {
      const {service} = action.payload
      const newState = state.map(s => {
        if (s.id === service.id) {
          return {...s, ...service}
        }

        return {...s}
      })

      return newState
    }

    case 'SET_ACTIVE_SERVICE': {
      const {source, service} = action.payload
      const services = state.filter(s => {
        return s.sourceID === source.id
      })
      return services.map(s => {
        const metadata = {active: s.id === service.id}
        s.metadata = metadata
        return s
      })
    }
  }

  return state
}

export default servicesReducer
