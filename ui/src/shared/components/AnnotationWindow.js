import React, {PropTypes} from 'react'

import * as schema from 'shared/schemas'
import * as style from 'src/shared/annotations/styles'

const AnnotationWindow = ({annotation, dygraph}) =>
  <div
    className="annotation-window"
    style={style.windowDimensions(annotation, dygraph)}
  />

const {shape} = PropTypes

AnnotationWindow.propTypes = {
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
}

export default AnnotationWindow
