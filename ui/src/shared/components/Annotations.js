import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Annotation from 'src/shared/components/Annotation'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'
import NewAnnotation from 'src/shared/components/NewAnnotation'

import {ADDING, TEMP_ANNOTATION} from 'src/shared/annotations/helpers'

import {
  addAnnotation,
  updateAnnotation,
  deleteAnnotation,
  addingAnnotationSuccess,
  dismissAddingAnnotation,
  mouseEnterTempAnnotation,
  mouseLeaveTempAnnotation,
} from 'src/shared/actions/annotations'
import {getAnnotations} from 'src/shared/annotations/helpers'

class Annotations extends Component {
  state = {
    dygraph: null,
  }

  componentDidMount() {
    this.props.annotationsRef(this)
  }

  render() {
    const {dygraph} = this.state
    const {
      mode,
      isTempHovering,
      handleAddAnnotation,
      handleUpdateAnnotation,
      handleDeleteAnnotation,
      handleDismissAddingAnnotation,
      handleAddingAnnotationSuccess,
      handleMouseEnterTempAnnotation,
      handleMouseLeaveTempAnnotation,
      staticLegendHeight,
    } = this.props

    if (!dygraph) {
      return null
    }

    const annotations = getAnnotations(dygraph, this.props.annotations)
    const tempAnnotation = this.props.annotations.find(
      a => a.id === TEMP_ANNOTATION.id
    )

    return (
      <div className="annotations-container">
        {mode === ADDING &&
          tempAnnotation &&
          <NewAnnotation
            dygraph={dygraph}
            tempAnnotation={tempAnnotation}
            onAddAnnotation={handleAddAnnotation}
            onDismissAddingAnnotation={handleDismissAddingAnnotation}
            onAddingAnnotationSuccess={handleAddingAnnotationSuccess}
            onUpdateAnnotation={handleUpdateAnnotation}
            isTempHovering={isTempHovering}
            onMouseEnterTempAnnotation={handleMouseEnterTempAnnotation}
            onMouseLeaveTempAnnotation={handleMouseLeaveTempAnnotation}
            staticLegendHeight={staticLegendHeight}
          />}
        {annotations.map(a =>
          <Annotation
            key={a.id}
            mode={mode}
            annotation={a}
            dygraph={dygraph}
            annotations={annotations}
            onUpdateAnnotation={handleUpdateAnnotation}
            onDeleteAnnotation={handleDeleteAnnotation}
            staticLegendHeight={staticLegendHeight}
          />
        )}
        {annotations.map((a, i) => {
          return a.duration
            ? <AnnotationWindow
                key={i}
                annotation={a}
                dygraph={dygraph}
                staticLegendHeight={staticLegendHeight}
              />
            : null
        })}
      </div>
    )
  }
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

Annotations.propTypes = {
  annotations: arrayOf(shape({})),
  mode: string,
  isTempHovering: bool,
  annotationsRef: func,
  handleDeleteAnnotation: func.isRequired,
  handleUpdateAnnotation: func.isRequired,
  handleAddAnnotation: func.isRequired,
  handleDismissAddingAnnotation: func.isRequired,
  handleAddingAnnotationSuccess: func.isRequired,
  handleMouseEnterTempAnnotation: func.isRequired,
  handleMouseLeaveTempAnnotation: func.isRequired,
  staticLegendHeight: number,
}

const mapStateToProps = ({
  annotations: {annotations, mode, isTempHovering},
}) => ({
  annotations,
  mode,
  isTempHovering,
})

const mapDispatchToProps = dispatch => ({
  handleAddingAnnotationSuccess: bindActionCreators(
    addingAnnotationSuccess,
    dispatch
  ),
  handleDismissAddingAnnotation: bindActionCreators(
    dismissAddingAnnotation,
    dispatch
  ),
  handleMouseEnterTempAnnotation: bindActionCreators(
    mouseEnterTempAnnotation,
    dispatch
  ),
  handleMouseLeaveTempAnnotation: bindActionCreators(
    mouseLeaveTempAnnotation,
    dispatch
  ),
  handleAddAnnotation: bindActionCreators(addAnnotation, dispatch),
  handleUpdateAnnotation: bindActionCreators(updateAnnotation, dispatch),
  handleDeleteAnnotation: bindActionCreators(deleteAnnotation, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(Annotations)
