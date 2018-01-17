import React, {PropTypes} from 'react'

const calcStyle = (annotation, dygraph) => {
  const [startX] = dygraph.xAxisRange()

  let windowStartXCoord = dygraph.toDomXCoord(annotation.time)

  if (annotation.time < startX) {
    windowStartXCoord = dygraph.toDomXCoord(startX)
  }

  const windowEndXCoord = dygraph.toDomXCoord(
    Number(annotation.time) + Number(annotation.duration)
  )
  const windowWidth = Number(windowEndXCoord) - Number(windowStartXCoord)

  const left = `${windowStartXCoord}px`
  const width = `${windowWidth}px`

  return {
    left,
    position: 'absolute',
    top: '0px',
    background:
      'linear-gradient(to bottom, rgba(255,0,0,0.3) 0%,rgba(255,0,0,0) 100%)',
    height: 'calc(100% - 20px)',
    borderTop: '2px solid #f00',
    width,
  }
}
const AnnotationWindow = ({annotation, dygraph}) =>
  <div
    className="dygraph-annotation-window"
    style={calcStyle(annotation, dygraph)}
  />

const {shape, string} = PropTypes

AnnotationWindow.propTypes = {
  annotation: shape({
    time: string.isRequired,
    duration: string.isRequired,
  }).isRequired,
  dygraph: shape({}).isRequired,
}

export default AnnotationWindow
