import React, {PropTypes} from 'react'

import AnnotationPoint from 'shared/components/AnnotationPoint'
import AnnotationSpan from 'shared/components/AnnotationSpan'

import * as schema from 'shared/schemas'

const Annotation = ({dygraph, annotation, mode}) =>
  <div>
    {annotation.startTime === annotation.endTime
      ? <AnnotationPoint
          annotation={annotation}
          mode={mode}
          dygraph={dygraph}
        />
      : <AnnotationSpan
          annotation={annotation}
          mode={mode}
          dygraph={dygraph}
        />}
  </div>

const {shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
}

export default Annotation
