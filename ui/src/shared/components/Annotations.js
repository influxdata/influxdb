import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Annotation from 'src/shared/components/Annotation'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'

import {updateAnnotation} from 'src/shared/actions/annotations'
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
    const {handleUpdateAnnotation} = this.props

    if (!dygraph) {
      return null
    }

    const annotations = getAnnotations(dygraph, this.props.annotations)

    return (
      <div className="annotations-container">
        {annotations.map((a, i) =>
          <Annotation
            key={i}
            annotation={a}
            dygraph={dygraph}
            onUpdateAnnotation={handleUpdateAnnotation}
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

const {arrayOf, func, shape} = PropTypes

Annotations.propTypes = {
  annotations: arrayOf(shape({})),
  annotationsRef: func,
  handleUpdateAnnotation: func.isRequired,
}

const mapStateToProps = ({annotations}) => ({
  annotations,
})

const mapDispatchToProps = dispatch => ({
  handleUpdateAnnotation: bindActionCreators(updateAnnotation, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(Annotations)
