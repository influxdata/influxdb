import {useLayoutEffect, MutableRefObject, CSSProperties} from 'react'

export const useLayoutStyle = (
  ref: MutableRefObject<HTMLElement>,
  f: (el: HTMLElement) => CSSProperties
) => {
  useLayoutEffect(() => {
    if (!ref.current) {
      return
    }

    const style = f(ref.current)

    for (const [k, v] of Object.entries(style)) {
      ref.current.style[k] = v
    }
  })
}
