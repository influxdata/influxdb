import {useRef, useState, useEffect} from 'react'

/*
  Use local state with a default value. The state will be reset to the default
  any time the default is updated.

  It's a "one way" binding between the default value and state, since updating
  the default will cause the state to update, but updating the state will not
  cause the default to update.
*/
export const useOneWayState = (defaultState: any) => {
  const isFirstRender = useRef(true)
  const [state, setState] = useState(defaultState)

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false

      return
    }

    setState(defaultState)
  }, [defaultState])

  return [state, setState]
}
