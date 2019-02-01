import {useState, useEffect, MutableRefObject} from 'react'

export const useMousePos = (
  ref: MutableRefObject<Element>
): [number, number] => {
  const [[x, y], setXY] = useState([null, null])

  useEffect(
    () => {
      if (!ref.current) {
        return
      }

      const onMouseEnter = e => {
        const {left, top} = ref.current.getBoundingClientRect()

        setXY([e.pageX - left, e.pageY - top])
      }

      const onMouseMove = onMouseEnter

      const onMouseLeave = () => {
        setXY([null, null])
      }

      ref.current.addEventListener('mouseenter', onMouseEnter)
      ref.current.addEventListener('mousemove', onMouseMove)
      ref.current.addEventListener('mouseleave', onMouseLeave)

      return () => {
        ref.current.removeEventListener('mouseenter', onMouseEnter)
        ref.current.removeEventListener('mousemove', onMouseMove)
        ref.current.removeEventListener('mouseleave', onMouseLeave)
      }
    },
    [ref.current]
  )

  return [x, y]
}
