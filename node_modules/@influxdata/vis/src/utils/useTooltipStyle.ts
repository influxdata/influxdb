import {useLayoutStyle} from './useLayoutStyle'
import {useRefMousePos} from './useMousePos'

const MARGIN_X = 30

export const useTooltipStyle = (el: HTMLDivElement) => {
  const {x, y} = useRefMousePos(document.body)

  // Position the tooltip next to the mouse cursor, like this:
  //
  //                   ┌─────────────┐
  //                   │             │
  //          (mouse)  │   tooltip   │
  //                   │             │
  //                   └─────────────┘
  //
  // The positioning is subject to the following restrictions:
  //
  // - If the tooltip overflows the right side of the screen, position it on
  //   the left side of the cursor instead
  //
  // - If the tooltip overflows the top or bottom of the screen (with a bit of
  //   margin), shift it just enough so that it is fully back inside the screen
  //
  useLayoutStyle(el, ({offsetWidth, offsetHeight}) => {
    if (x === null || y === null) {
      return {
        display: 'none',
      }
    }

    let dx = MARGIN_X
    let dy = 0 - offsetHeight / 2

    if (x + dx + offsetWidth > window.innerWidth) {
      dx = 0 - MARGIN_X - offsetWidth
    }

    if (y + dy + offsetHeight > window.innerHeight) {
      dy -= y + dy + offsetHeight - window.innerHeight
    }

    if (y + dy < 0) {
      dy += 0 - (y + dy)
    }

    return {
      display: 'inline',
      position: 'fixed',
      left: `${x + dx}px`,
      top: `${y + dy}px`,
    }
  })
}
