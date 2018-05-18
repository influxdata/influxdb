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

      return state.filter(s => s.id !== service.id)
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
    }
  }

  return state
}

export default servicesReducer
