import React, {PropTypes} from 'react'

const calcStyle = (annotation, dygraph) => {
  const left = `${dygraph.toDomXCoord(annotation.time)}px`

  return {
    left,
    position: 'absolute',
    top: '0px',
    backgroundColor: '#f00',
    height: '100%',
    width: '2px',
    transform: 'translateX(-50%)',
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
