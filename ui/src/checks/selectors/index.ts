import {AppState, Check} from 'src/types'

export const getCheck = (state: AppState, id: string): Check => {
  const checksList = state.checks.list
  return checksList.find(c => c.id === id)
}

export const getCheckIDs = (state: AppState): {[x: string]: boolean} => {
  return state.checks.list.reduce(
    (acc, check) => ({...acc, [check.id]: true}),
    {}
  )
}
