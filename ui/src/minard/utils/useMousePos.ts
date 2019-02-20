import {useState, useEffect} from 'react'

export const useMousePos = (el: Element): {x: number; y: number} => {
  const [state, setState] = useState({x: null, y: null})

  useEffect(() => {
    if (!el) {
      return
    }

    const onMouseEnter = e => {
      const {left, top} = el.getBoundingClientRect()

      setState({x: e.pageX - left, y: e.pageY - top})
    }

    const onMouseMove = onMouseEnter

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
