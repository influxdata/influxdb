import {useReducer} from 'react'

export const useForceUpdate = () => {
  return useReducer(count => count + 1, 0)[1] as () => void
}
