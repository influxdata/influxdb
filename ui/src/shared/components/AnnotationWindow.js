import React, {PropTypes} from 'react'

import * as schema from 'shared/schemas'
import {annotationWindowStyle} from 'src/shared/annotations/styles'

const AnnotationWindow = ({annotation, dygraph}) =>
  <div
    className="dygraph-annotation-window"
    style={annotationWindowStyle(annotation, dygraph)}
  />

const {shape} = PropTypes

AnnotationWindow.propTypes = {
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
}

export default AnnotationWindow
