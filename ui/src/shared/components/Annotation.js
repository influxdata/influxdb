import React from 'react'
import PropTypes from 'prop-types'

import AnnotationPoint from 'shared/components/AnnotationPoint'
import AnnotationSpan from 'shared/components/AnnotationSpan'

import * as schema from 'shared/schemas'

const Annotation = ({
  mode,
  dygraph,
  dWidth,
  annotation,
  staticLegendHeight,
}) => (
  <div>
    {annotation.startTime === annotation.endTime ? (
      <AnnotationPoint
        mode={mode}
        dygraph={dygraph}
        annotation={annotation}
        dWidth={dWidth}
        staticLegendHeight={staticLegendHeight}
      />
    ) : (
      <AnnotationSpan
        mode={mode}
        dygraph={dygraph}
        annotation={annotation}
        dWidth={dWidth}
        staticLegendHeight={staticLegendHeight}
      />
    )}
  </div>
)

const {number, shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  dWidth: number,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  staticLegendHeight: number,
}

export default Annotation
