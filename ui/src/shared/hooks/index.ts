import React, {Dispatch, useContext} from 'react'

import * as Rule from 'src/alerting/components/notifications/RuleOverlay.reducer'

type RuleMode = 'NewRuleDispatch' | 'EditRuleDispatch'

export const RuleMode = React.createContext<RuleMode>(null)
export const NewRuleDispatch = React.createContext<Dispatch<Rule.Action>>(null)
export const EditRuleDispatch = React.createContext<Dispatch<Rule.Action>>(null)

interface Contexts {
  NewRuleDispatch: typeof NewRuleDispatch
  EditRuleDispatch: typeof EditRuleDispatch
}

export const contexts: Contexts = {
  NewRuleDispatch,
  EditRuleDispatch,
}

export const useRuleDispatch = () => {
  const mode = useContext(RuleMode)

  return useContext(contexts[mode])
}
