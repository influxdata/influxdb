import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Annotation from 'src/shared/components/Annotation'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'

import {
  addAnnotation,
  updateAnnotation,
  deleteAnnotation,
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
    const {mode, handleUpdateAnnotation, handleDeleteAnnotation} = this.props

    if (!dygraph) {
      return null
    }

    const annotations = getAnnotations(dygraph, this.props.annotations)

    return (
      <div className="annotations-container">
        {annotations.map(a =>
          <Annotation
            key={a.id}
            mode={mode}
            annotation={a}
            dygraph={dygraph}
            annotations={annotations}
            onUpdateAnnotation={handleUpdateAnnotation}
            onDeleteAnnotation={handleDeleteAnnotation}
          />
        )}
        {annotations.map((a, i) => {
          return a.duration
            ? <AnnotationWindow key={i} annotation={a} dygraph={dygraph} />
            : null
        })}
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

Annotations.propTypes = {
  annotations: arrayOf(shape({})),
  mode: string,
  annotationsRef: func,
  handleDeleteAnnotation: func.isRequired,
  handleUpdateAnnotation: func.isRequired,
  handleAddAnnotation: func.isRequired,
}

const mapStateToProps = ({annotations}) => ({
  annotations,
})

const mapDispatchToProps = dispatch => ({
  handleAddAnnotation: bindActionCreators(addAnnotation, dispatch),
  handleUpdateAnnotation: bindActionCreators(updateAnnotation, dispatch),
  handleDeleteAnnotation: bindActionCreators(deleteAnnotation, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(Annotations)
