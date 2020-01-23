import {AppState} from 'src/types'

export const getRuleIDs = (state: AppState): {[x: string]: boolean} => {
  return state.rules.list.reduce((acc, rule) => ({...acc, [rule.id]: true}), {})
}
