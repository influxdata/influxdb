import React, {PropTypes} from 'react'

import AnnotationPoint from 'shared/components/AnnotationPoint'
import AnnotationSpan from 'shared/components/AnnotationSpan'

import * as schema from 'shared/schemas'

const Annotation = ({dygraph, annotation, mode, lastUpdated}) =>
  <div>
    {annotation.startTime === annotation.endTime
      ? <AnnotationPoint
          lastUpdated={lastUpdated}
          annotation={annotation}
          mode={mode}
          dygraph={dygraph}
        />
      : <AnnotationSpan
          lastUpdated={lastUpdated}
          annotation={annotation}
          mode={mode}
          dygraph={dygraph}
        />}
  </div>

const {number, shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  lastUpdated: number,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
}

export default Annotation
