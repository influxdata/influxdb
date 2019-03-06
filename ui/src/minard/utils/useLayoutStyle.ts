import {useLayoutEffect, CSSProperties} from 'react'

export const useLayoutStyle = (
  el: HTMLElement,
  f: (el: HTMLElement) => CSSProperties
) => {
  useLayoutEffect(() => {
    if (!el) {
      return
    }

    const style = f(el)

    for (const [k, v] of Object.entries(style)) {
      el.style[k] = v
    }
  })
}
