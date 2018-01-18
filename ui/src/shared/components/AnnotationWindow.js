import React, {PropTypes} from 'react'

import {annotationWindowStyle} from 'src/shared/annotations/styles'

const AnnotationWindow = ({annotation, dygraph}) =>
  <div
    className="dygraph-annotation-window"
    style={annotationWindowStyle(annotation, dygraph)}
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
