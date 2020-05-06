import {useRef, useEffect} from 'react'

import {useTooltipStyle} from '../utils/useTooltipStyle'

/*
  Returns a DOM node that a tooltip can be rendered inside.

  The node will be created and appended to the end of the document body on
  mount. After every render, the tooltip is automatically positioned to be next
  to the mouse. It will be destroyed on unmount.  

  The returned node is intended to be used with `React.createPortal`. It is
  appended to the end of the document to circumvent z-index issues.
*/
export const useTooltipElement = () => {
  const ref = useRef<HTMLDivElement>(null)

  if (ref.current === null) {
    ref.current = document.createElement('div')
    ref.current.classList.add('vis-tooltip-container')

    document.body.appendChild(ref.current)
  }

  useEffect(() => () => document.body.removeChild(ref.current), [])

  useTooltipStyle(ref.current)

  return ref.current
}
