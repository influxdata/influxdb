import {useRef, useReducer, useEffect} from 'react'

// Minimum number of pixels a user must brush before we decide whether the
// action is a vertical or horizontal brush
const MIN_BRUSH_DELTA = 5

export interface DragEvent {
  type: 'drag' | 'dragend'
  initialX: number
  initialY: number
  dragMode: 'brushX' | 'brushY' | 'pan' | null
  x: number
  y: number
  shiftKey: boolean
  ctrlKey: boolean
  metaKey: boolean
}

export const useDragEvent = (el: HTMLElement): DragEvent | null => {
  const dragJustEnded = useRef(false)
  const isActive = useRef(false)

  const [event, dispatch] = useReducer(
    (state: DragEvent | null, {type, e}) => {
      const {left, top} = el.getBoundingClientRect()
      const x = e.pageX - left
      const y = e.pageY - top

      switch (type) {
        case 'mousedown': {
          return {
            type: 'drag',
            initialX: x,
            initialY: y,
            x,
            y,
            dragMode: null,
            shiftKey: e.shiftKey,
            metaKey: e.metaKey,
            ctrlKey: e.ctrlKey,
          }
        }

        case 'mousemove': {
          let dragMode = state.dragMode

          if (!dragMode) {
            const dx = Math.abs(x - state.initialX)
            const dy = Math.abs(y - state.initialY)

            if (dx >= dy && dx > MIN_BRUSH_DELTA) {
              dragMode = 'brushX'
            } else if (dy > MIN_BRUSH_DELTA) {
              dragMode = 'brushY'
            }
          }

          return {
            ...state,
            type: 'drag',
            dragMode,
            x,
            y,
          }
        }

        case 'mouseup': {
          return {
            ...state,
            type: 'dragend',
            x,
            y,
          }
        }

        default: {
          return null
        }
      }
    },
    null as never
  )

  useEffect(() => {
    if (!el) {
      return
    }

    const onMouseMove = (e: MouseEvent) => {
      dispatch({type: 'mousemove', e})
    }

    const onMouseDown = (e: MouseEvent) => {
      document.addEventListener('mousemove', onMouseMove)
      dragJustEnded.current = false
      isActive.current = true
      dispatch({type: 'mousedown', e})
    }

    const onMouseUp = (e: MouseEvent) => {
      document.removeEventListener('mousemove', onMouseMove)
      dragJustEnded.current = true
      dispatch({type: 'mouseup', e})
    }

    el.addEventListener('mousedown', onMouseDown)
    document.addEventListener('mouseup', onMouseUp)

    return () => {
      el.removeEventListener('mousedown', onMouseDown)
      document.removeEventListener('mouseup', onMouseUp)
      document.removeEventListener('mousemove', onMouseMove)
    }
  }, [el])

  if (!isActive.current) {
    return null
  }

  if (dragJustEnded.current) {
    isActive.current = false
    return event
  }

  return event
}
