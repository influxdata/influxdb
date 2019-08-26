// Libraries
import React, {
  FC,
  Dispatch,
  createContext,
  useReducer,
  useRef,
  useContext,
} from 'react'

// Reducer
import {Action} from './RuleOverlay.actions'
import {RuleState, reducer} from './RuleOverlay.reducer'

const RuleStateContext = createContext<RuleState>(null)
const RuleDispatchContext = createContext<Dispatch<Action>>(null)

export const useRuleState = (): RuleState => {
  return useContext(RuleStateContext)
}

export const useRuleDispatch = (): Dispatch<Action> => {
  return useContext(RuleDispatchContext)
}

const RuleOverlayProvider: FC<{initialState: RuleState}> = ({
  initialState,
  children,
}) => {
  const prevInitialStateRef = useRef(initialState)

  const [state, dispatch] = useReducer((state: RuleState, action: Action) => {
    if (prevInitialStateRef.current !== initialState) {
      prevInitialStateRef.current = initialState

      return initialState
    }

    return reducer(state, action)
  }, initialState)

  return (
    <RuleStateContext.Provider value={state}>
      <RuleDispatchContext.Provider value={dispatch}>
        {children}
      </RuleDispatchContext.Provider>
    </RuleStateContext.Provider>
  )
}

export default RuleOverlayProvider
