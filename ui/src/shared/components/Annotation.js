import React, {PropTypes} from 'react'

const calcStyle = ({time}, dygraph) => {
  const [startX, endX] = dygraph.xAxisRange()
  let visibility = 'visible'

  if (time < startX || time > endX) {
    visibility = 'hidden'
  }

  const containerLeftPadding = 16
  const left = `${dygraph.toDomXCoord(time) + containerLeftPadding}px`
  const width = 2

  return {
    left,
    position: 'absolute',
    top: '8px',
    backgroundColor: '#f00',
    height: 'calc(100% - 36px)',
    width: `${width}px`,
    transform: `translateX(-${width / 2}px)`, // translate should always be half with width to horizontally center the annotation pole
    visibility,
    zIndex: '3',
  }
}

const triangleStyle = {
  position: 'absolute',
  top: '3px',
  left: '-6px',
  border: '7px solid transparent',
  borderTopColor: '#f00',
  transform: 'scaleY(1.5)',
  cursor: 'pointer',
}

const Annotation = ({annotation, dygraph}) =>
  <div
    className="dygraph-annotation"
    style={calcStyle(annotation, dygraph)}
    data-time-ms={annotation.time}
    data-time-local={new Date(+annotation.time)}
  >
    <div style={triangleStyle} />
  </div>

const {shape, string} = PropTypes

Annotation.propTypes = {
  annotation: shape({
    time: string.isRequired,
    duration: string,
  }).isRequired,
  dygraph: shape({}).isRequired,
}

export default Annotation
