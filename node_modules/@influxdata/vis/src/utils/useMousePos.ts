import {useState, useEffect, useMemo, MouseEvent} from 'react'

export interface MousePosition {
  x: number | null
  y: number | null
}

interface UseMousePosProps {
  onMouseEnter: (e: MouseEvent<HTMLDivElement>) => void
  onMouseMove: (e: MouseEvent<HTMLDivElement>) => void
  onMouseLeave: (e: MouseEvent<HTMLDivElement>) => void
}

const mousePositionFromEvent = (e: MouseEvent<HTMLDivElement>) => {
  const {top, left} = e.currentTarget.getBoundingClientRect()

  return {x: e.pageX - left, y: e.pageY - top}
}

export const useMousePos = (): [MousePosition, UseMousePosProps] => {
  const [state, setState] = useState<MousePosition>({x: null, y: null})

  const eventProps = useMemo(
    () => ({
      onMouseEnter(e) {
        setState(mousePositionFromEvent(e))
      },
      onMouseMove(e) {
        setState(mousePositionFromEvent(e))
      },
      onMouseLeave() {
        setState({x: null, y: null})
      },
    }),
    []
  )

  return [state, eventProps]
}

export const useRefMousePos = (el: Element): MousePosition => {
  const [state, setState] = useState({x: null, y: null})

  useEffect(() => {
    if (!el) {
      // Force one more render to give the ref a chance to attach
      setState({x: null, y: null})

      return
    }

    const onMouseEnter = e => {
      const {left, top} = el.getBoundingClientRect()

      setState({x: e.pageX - left, y: e.pageY - top})
    }

    const onMouseMove = e => {
      onMouseEnter(e)
    }

    const onMouseLeave = () => {
      setState({x: null, y: null})
    }

    el.addEventListener('mouseenter', onMouseEnter)
    el.addEventListener('mousemove', onMouseMove)
    el.addEventListener('mouseleave', onMouseLeave)

    return () => {
      el.removeEventListener('mouseenter', onMouseEnter)
      el.removeEventListener('mousemove', onMouseMove)
      el.removeEventListener('mouseleave', onMouseLeave)
    }
  }, [el])

  return state
}
