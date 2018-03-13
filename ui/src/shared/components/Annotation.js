import React from 'react'
import PropTypes from 'prop-types'

import AnnotationPoint from 'shared/components/AnnotationPoint'
import AnnotationSpan from 'shared/components/AnnotationSpan'

import * as schema from 'shared/schemas'

const Annotation = ({
  dygraph,
  annotation,
  mode,
  lastUpdated,
  staticLegendHeight,
}) =>
  <div>
    {annotation.startTime === annotation.endTime
      ? <AnnotationPoint
          lastUpdated={lastUpdated}
          annotation={annotation}
          mode={mode}
          dygraph={dygraph}
          staticLegendHeight={staticLegendHeight}
        />
      : <AnnotationSpan
          lastUpdated={lastUpdated}
          annotation={annotation}
          mode={mode}
          dygraph={dygraph}
          staticLegendHeight={staticLegendHeight}
        />}
  </div>

const {number, shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  lastUpdated: number,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  staticLegendHeight: number,
}

export default Annotation
