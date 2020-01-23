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
import {EndpointState, Action, reducer} from './EndpointOverlay.reducer'

const EndpointStateContext = createContext<EndpointState>(null)
const EndpointDispatchContext = createContext<Dispatch<Action>>(null)

export const EndpointOverlayProvider: FC<{initialState: EndpointState}> = ({
  initialState,
  children,
}) => {
  const prevInitialStateRef = useRef(initialState)

  const [state, dispatch] = useReducer(
    (state: EndpointState, action: Action) => {
      if (prevInitialStateRef.current !== initialState) {
        prevInitialStateRef.current = initialState

        return initialState
      }

      return reducer(state, action)
    },
    initialState
  )

  return (
    <EndpointStateContext.Provider value={state}>
      <EndpointDispatchContext.Provider value={dispatch}>
        {children}
      </EndpointDispatchContext.Provider>
    </EndpointStateContext.Provider>
  )
}

export const useEndpointState = (): EndpointState => {
  return useContext(EndpointStateContext)
}

export const useEndpointDispatch = (): Dispatch<Action> => {
  return useContext(EndpointDispatchContext)
}

export const useEndpointReducer = (): [EndpointState, Dispatch<Action>] => {
  return [useEndpointState(), useEndpointDispatch()]
}
