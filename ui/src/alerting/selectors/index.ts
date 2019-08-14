import {AppState, Check} from 'src/types'

export const getCheck = (state: AppState, id: string): Check => {
  const checksList = state.checks.list
  return checksList.find(c => c.id === id)
}
