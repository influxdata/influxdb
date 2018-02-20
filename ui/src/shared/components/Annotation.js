import React, {Component, PropTypes} from 'react'

import AnnotationPoint from 'shared/components/AnnotationPoint'
import AnnotationSpan from 'shared/components/AnnotationSpan'

import * as schema from 'shared/schemas'

class Annotation extends Component {
  render() {
    const {dygraph, annotation, mode, onUpdateAnnotation} = this.props

    return (
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
              onUpdateAnnotation={onUpdateAnnotation}
              dygraph={dygraph}
            />}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  onUpdateAnnotation: func.isRequired,
}

export default Annotation
