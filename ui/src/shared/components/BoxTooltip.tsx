// Libraries
import React, {useRef, useLayoutEffect, FunctionComponent} from 'react'
import {createPortal} from 'react-dom'

// Constants
import {TOOLTIP_PORTAL_ID} from 'src/shared/components/TooltipPortal'

// Styles
import 'src/shared/components/BoxTooltip.scss'

interface Props {
  triggerRect: DOMRect
  children: JSX.Element
}

const BoxTooltip: FunctionComponent<Props> = ({triggerRect, children}) => {
  const ref = useRef<HTMLDivElement>(null)

  // Position the tooltip after it has rendered, taking into account its size
  useLayoutEffect(() => {
    const el = ref.current

    if (!el || !triggerRect) {
      return
    }

    const rect = el.getBoundingClientRect()

    // Always position the tooltip to the left of the trigger position
    const left = triggerRect.left - rect.width

    // Attempt to position the vertical midpoint of tooltip next to the
    // vertical midpoint of the trigger rectangle
    let top = triggerRect.top + triggerRect.height / 2 - rect.height / 2

    // If the tooltip overflows the top of the screen, align the top of
    // the tooltip with the top of the screen
    if (top < 0) {
      top = 0
    }

    // If the tooltip overflows the bottom of the screen, align the bottom of
    // the tooltip with the bottom of the screen
    if (top + rect.height > window.innerHeight) {
      top = window.innerHeight - rect.height
    }

    el.setAttribute(
      'style',
      `visibility: visible; top: ${top}px; left: ${left}px;`
    )

    // Position the caret (little arrow on the side of the tooltip) so that it
    // points to the vertical midpoint of the trigger rectangle
    const caretTop = triggerRect.top + triggerRect.height / 2 - top

    el.querySelector('.box-tooltip--caret').setAttribute(
      'style',
      `top: ${caretTop}px;`
    )
  })

  return createPortal(
    <div className="box-tooltip left" ref={ref}>
      {children}
      <div className="box-tooltip--caret" />
    </div>,
    document.querySelector(`#${TOOLTIP_PORTAL_ID}`)
  )
}

export default BoxTooltip
