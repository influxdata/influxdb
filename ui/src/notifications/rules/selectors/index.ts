import {AppState} from 'src/types'

export const getRuleIDs = (state: AppState): {[x: string]: boolean} => {
  return state.resources.rules.allIDs.reduce(
    (acc, ruleID) => ({...acc, [ruleID]: true}),
    {}
  )
}
