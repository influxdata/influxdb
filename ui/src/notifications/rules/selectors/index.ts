import {AppState, NotificationRuleDraft} from 'src/types'

export const getRuleIDs = (state: AppState): {[x: string]: boolean} => {
  return state.resources.rules.allIDs.reduce(
    (acc, ruleID) => ({...acc, [ruleID]: true}),
    {}
  )
}

export const sortRulesByName = (
  rules: NotificationRuleDraft[]
): NotificationRuleDraft[] =>
  rules.sort((a, b) => (a.name.toLowerCase() > b.name.toLowerCase() ? 1 : -1))
