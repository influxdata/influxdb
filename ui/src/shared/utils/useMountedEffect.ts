import {
  useEffect,
  useLayoutEffect,
  useRef,
  EffectCallback,
  DependencyList,
} from 'react'

/*
  Behaves like `useEffect`, but won't fire after the initial render of a
  component.
*/
export const useMountedEffect = (
  effect: EffectCallback,
  inputs?: DependencyList
) => {
  const isFirstRender = useRef(true)

  useEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false

      return
    }

    return effect()
  }, inputs)
}

export const useMountedLayoutEffect = (
  effect: EffectCallback,
  inputs?: DependencyList
) => {
  const isFirstRender = useRef(true)

  useLayoutEffect(() => {
    if (isFirstRender.current) {
      isFirstRender.current = false

      return
    }

    return effect()
  }, inputs)
}
