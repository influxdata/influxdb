import {useRef, DependencyList} from 'react'

/*
  A hook that works like `useMemo`, but takes an additional boolean argument.
  The factory will not be called until the boolean is true; until then, `null`
  is returned.
*/
export const useLazyMemo = <T>(
  factory: () => T,
  deps: DependencyList,
  canCompute: boolean
): T | null => {
  const hasComputed = useRef(false)
  const result = useRef<T>(null)
  const prevDeps = useRef(deps)

  if (!canCompute) {
    return null
  }

  if (!hasComputed.current) {
    hasComputed.current = true
    result.current = factory()
    prevDeps.current = deps

    return result.current
  }

  const depsChanged = deps.some((x, i) => x !== prevDeps.current[i])

  if (depsChanged) {
    result.current = factory()
    prevDeps.current = deps
  }

  return result.current
}
