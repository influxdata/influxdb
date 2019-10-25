// Libraries
import React, {useRef, useLayoutEffect, FunctionComponent} from 'react'
import {createPortal} from 'react-dom'

// Constants
import {TOOLTIP_PORTAL_ID} from 'src/portals/TooltipPortal'

// Types
import {ComponentColor} from '@influxdata/clockface'

interface Props {
  triggerRect: DOMRect
  children: JSX.Element
  maxWidth?: number
  color?: ComponentColor
}

const BoxTooltip: FunctionComponent<Props> = ({
  triggerRect,
  children,
  maxWidth = 300,
  color = ComponentColor.Primary,
}) => {
  const ref = useRef<HTMLDivElement>(null)

  // Position the tooltip after it has rendered, taking into account its size
  useLayoutEffect(() => {
    const el = ref.current

    if (!el || !triggerRect) {
      return
    }

    const rect = el.getBoundingClientRect()
    let left = Math.floor(triggerRect.left - rect.width) - 2
    // Hacky for #15518 (https://github.com/influxdata/influxdb/issues/15518)
    // Long term solution would be to have getBoundingClientRect output
    // to the left / ride side of the div based on boxtooltip location
    let caretClassName = 'left'

    // If the width of the tooltip causes it to overflow left
    // position it to the right of the trigger element
    if (left < 0) {
      left = triggerRect.left + triggerRect.width
      caretClassName = 'right'
    }

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
      `visibility: visible; top: ${top}px; left: ${left}px; max-width: ${maxWidth}px;`
    )

    // Position the caret (little arrow on the side of the tooltip) so that it
    // points to the vertical midpoint of the trigger rectangle
    const caretTop = triggerRect.top + triggerRect.height / 2 - top

    el.querySelector('.box-tooltip--caret').setAttribute(
      'style',
      `top: ${caretTop}px;`
    )

    el.className = `box-tooltip  box-tooltip__${color} ${caretClassName}`
  })

  return createPortal(
    <div
      className={`box-tooltip box-tooltip__${color}`}
      ref={ref}
      style={{maxWidth}}
    >
      {children}
      <div className="box-tooltip--caret-container">
        <div className="box-tooltip--caret" />
      </div>
    </div>,
    document.querySelector(`#${TOOLTIP_PORTAL_ID}`)
  )
}

export default BoxTooltip
