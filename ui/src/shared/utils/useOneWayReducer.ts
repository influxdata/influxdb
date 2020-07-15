import {
  useRef,
  useReducer,
  useCallback,
  Reducer,
  ReducerState,
  ReducerAction,
  Dispatch,
} from 'react'

/*
  Works like `useReducer`, except if the `defaultState` argument changes then
  the reducer state will reset to `defaultState`.

  It is assumed that the passed `reducer` function has a stable identity over
  the lifetime of the component.
*/
export const useOneWayReducer = <R extends Reducer<any, any>>(
  reducer: R,
  defaultState: ReducerState<R>
): [ReducerState<R>, Dispatch<ReducerAction<R>>] => {
  // The value of `defaultState` the last time the hook was called
  const prevDefaultState = useRef(defaultState)

  // Whether or not the next run of the reducer should be against its internal
  // state, or against the defaultState
  const reduceDefaultState = useRef(false)

  const wrappedReducer = useCallback(
    (state: ReducerState<R>, action: ReducerAction<R>) => {
      if (reduceDefaultState.current) {
        reduceDefaultState.current = false

        return reducer(prevDefaultState.current, action)
      }

      return reducer(state, action)
    },
    [reducer]
  )

  const [reducerState, dispatch] = useReducer(wrappedReducer, defaultState)

  if (defaultState !== prevDefaultState.current) {
    reduceDefaultState.current = true
    prevDefaultState.current = defaultState

    return [defaultState, dispatch]
  }

  return [reducerState, dispatch]
}
