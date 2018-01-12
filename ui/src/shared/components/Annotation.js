import React, {PropTypes} from 'react'

const nanosToMillis = time => {
  return time.substring(0, time.length - 6)
}

const calcStyle = (annotation, dygraph) => {
  const millis = nanosToMillis(annotation.time)
  const left = `${dygraph.toDomXCoord(millis)}px`
  const width = 2

  return {
    left,
    position: 'absolute',
    top: '0px',
    backgroundColor: '#f00',
    height: '100%',
    width: `${width}px`,
    transform: `translateX(-${width / 2}px)`, // translate should always be half with width to horizontally center the annotation pole
    zIndex: '10',
  }
}
const Annotation = ({annotation, dygraph}) =>
  <div className="dygraph-annotation" style={calcStyle(annotation, dygraph)} />

const {shape, string} = PropTypes

Annotation.propTypes = {
  annotation: shape({
    time: string.isRequired,
  }).isRequired,
  dygraph: shape({}).isRequired,
}

export default Annotation
