import * as React from 'react'
import {useCallback} from 'react'

export interface DragEvent {
  x: number
  y: number
  type: 'dragStart' | 'drag' | 'dragStop'
}

type MouseDownEvent = React.MouseEvent<Element, MouseEvent>

export const useDragBehavior = (
  onDrag: (e: DragEvent) => any
): {onMouseDown: (e: MouseDownEvent) => any} => {
  const onMouseDown = useCallback(
    (mouseDownEvent: MouseDownEvent) => {
      mouseDownEvent.stopPropagation()

      onDrag({
        type: 'dragStart',
        x: mouseDownEvent.pageX,
        y: mouseDownEvent.pageY,
      })

      const onMouseMove = mouseMoveEvent => {
        onDrag({
          type: 'drag',
          x: mouseMoveEvent.pageX,
          y: mouseMoveEvent.pageY,
        })
      }

      const onMouseUp = mouseUpEvent => {
        document.removeEventListener('mousemove', onMouseMove)
        document.removeEventListener('mouseup', onMouseUp)

        onDrag({
          type: 'dragStop',
          x: mouseUpEvent.pageX,
          y: mouseUpEvent.pageY,
        })
      }

      document.addEventListener('mousemove', onMouseMove)
      document.addEventListener('mouseup', onMouseUp)
    },
    [onDrag]
  )

  return {onMouseDown}
}
