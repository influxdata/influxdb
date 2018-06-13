import React, {Component} from 'react'
import {connect} from 'react-redux'

import Annotation from 'src/shared/components/Annotation'
import NewAnnotation from 'src/shared/components/NewAnnotation'

import {ADDING, TEMP_ANNOTATION} from 'src/shared/annotations/helpers'

import {
  updateAnnotation,
  addingAnnotationSuccess,
  dismissAddingAnnotation,
  mouseEnterTempAnnotation,
  mouseLeaveTempAnnotation,
} from 'src/shared/actions/annotations'
import {visibleAnnotations} from 'src/shared/annotations/helpers'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AnnotationInterface, DygraphClass} from 'src/types'

interface Props {
  dygraph: DygraphClass
  dWidth: number
  xAxisRange: number[]
  staticLegendHeight: number
  annotations: AnnotationInterface[]
  mode: string
  isTempHovering: boolean
  handleUpdateAnnotation: () => void
  handleDismissAddingAnnotation: () => void
  handleAddingAnnotationSuccess: () => void
  handleMouseEnterTempAnnotation: () => void
  handleMouseLeaveTempAnnotation: () => void
}

@ErrorHandling
class Annotations extends Component<Props> {
  public render() {
    const {
      mode,
      dWidth,
      dygraph,
      xAxisRange,
      isTempHovering,
      handleUpdateAnnotation,
      handleDismissAddingAnnotation,
      handleAddingAnnotationSuccess,
      handleMouseEnterTempAnnotation,
      handleMouseLeaveTempAnnotation,
      staticLegendHeight,
    } = this.props
    console.log('rendering annotations')
    return (
      <div className="annotations-container">
        {mode === ADDING &&
          this.tempAnnotation && (
            <NewAnnotation
              dygraph={dygraph}
              isTempHovering={isTempHovering}
              tempAnnotation={this.tempAnnotation}
              staticLegendHeight={staticLegendHeight}
              onUpdateAnnotation={handleUpdateAnnotation}
              onDismissAddingAnnotation={handleDismissAddingAnnotation}
              onAddingAnnotationSuccess={handleAddingAnnotationSuccess}
              onMouseEnterTempAnnotation={handleMouseEnterTempAnnotation}
              onMouseLeaveTempAnnotation={handleMouseLeaveTempAnnotation}
            />
          )}
        {this.annotations.map(a => (
          <Annotation
            key={a.id}
            mode={mode}
            annotation={a}
            dygraph={dygraph}
            dWidth={dWidth}
            xAxisRange={xAxisRange}
            staticLegendHeight={staticLegendHeight}
          />
        ))}
      </div>
    )
  }

  get annotations() {
    return visibleAnnotations(
      this.props.xAxisRange,
      this.props.annotations
    ).filter(a => a.id !== TEMP_ANNOTATION.id)
  }

  get tempAnnotation() {
    return this.props.annotations.find(a => a.id === TEMP_ANNOTATION.id)
  }
}

const mstp = ({annotations: {annotations, mode, isTempHovering}}) => ({
  annotations,
  mode: mode || 'NORMAL',
  isTempHovering,
})

const mdtp = {
  handleAddingAnnotationSuccess: addingAnnotationSuccess,
  handleDismissAddingAnnotation: dismissAddingAnnotation,
  handleMouseEnterTempAnnotation: mouseEnterTempAnnotation,
  handleMouseLeaveTempAnnotation: mouseLeaveTempAnnotation,
  handleUpdateAnnotation: updateAnnotation,
}

export default connect(mstp, mdtp)(Annotations)
