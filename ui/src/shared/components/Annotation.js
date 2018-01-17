import React, {PropTypes} from 'react'

const calcStyle = ({time}, dygraph) => {
  const [startX] = dygraph.xAxisRange()
  let visibility = 'visible'

  if (time < startX) {
    visibility = 'hidden'
  }

  const left = `${dygraph.toDomXCoord(time)}px`
  const width = 2

  return {
    left,
    position: 'absolute',
    top: '0px',
    backgroundColor: '#f00',
    height: 'calc(100% - 20px)',
    width: `${width}px`,
    transform: `translateX(-${width / 2}px)`, // translate should always be half with width to horizontally center the annotation pole
    visibility,
  }
}

const Annotation = ({annotation, dygraph}) =>
  <div className="dygraph-annotation" style={calcStyle(annotation, dygraph)} />

const {shape, string} = PropTypes

Annotation.propTypes = {
  annotation: shape({
    time: string.isRequired,
    duration: string,
  }).isRequired,
  dygraph: shape({}).isRequired,
}

export default Annotation
