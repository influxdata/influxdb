import {useRef} from 'react'

/*
  If the supplied set of elements `s` is equal to the previously supplied set
  of elements, return the previously supplied set.

  The two sets are considered equal if each set contains the other.

  Similar to `useMemo`, this method is useful for preserving the identity of
  data across rerenders. Unlike `useMemo`, it checks the logical identity of
  the new data, rather than the reference identity.
*/
export const useSetIdentity = <T extends any[]>(s: T): T => {
  const ref = useRef(s)
  const prevS = ref.current

  if (prevS !== s) {
    const isSetEqual =
      prevS.length === s.length && s.every(x => prevS.includes(x))

    if (!isSetEqual) {
      ref.current = s
    }
  }

  return ref.current
}
