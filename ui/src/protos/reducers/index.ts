// Types
import {Action} from 'src/protos/actions/'
import {Proto} from 'src/api'

export interface ProtosState {
  [protoName: string]: Proto
}

const protosReducer = (state: ProtosState = {}, action: Action) => {
  switch (action.type) {
    case 'LOAD_PROTO': {
      const {
        proto,
        proto: {name},
      } = action.payload

      return {
        ...state,
        [name]: {...proto},
      }
    }
  }

  return state
}

export default protosReducer
