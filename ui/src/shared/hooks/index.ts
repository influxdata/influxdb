import React, {useContext} from 'react'

import {ActionPayload} from 'src/alerting/components/notifications/RuleOverlay.reducer'

export enum RuleMode {
  New = 'NewRuleDispatch',
  Edit = 'EditRuleDispatch',
}

export const RuleModeContext = React.createContext<RuleMode>(null)
export const NewRuleDispatch = React.createContext<
  (action: ActionPayload) => void
>(null)
export const EditRuleDispatch = React.createContext<
  (action: ActionPayload) => void
>(null)

interface Contexts {
  NewRuleDispatch: typeof NewRuleDispatch
  EditRuleDispatch: typeof EditRuleDispatch
}

export const contexts: Contexts = {
  NewRuleDispatch,
  EditRuleDispatch,
}

export const useRuleDispatch = () => {
  const mode = useContext(RuleModeContext)

  return useContext(contexts[mode])
}
