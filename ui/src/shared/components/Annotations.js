import React, {PropTypes, Component} from 'react'
import Annotation from 'src/shared/components/Annotation'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'

class Annotations extends Component {
  state = {
    dygraph: null,
  }

  componentDidMount() {
    this.props.annotationsRef(this)
  }

  render() {
    const {dygraph} = this.state
    const {annotations} = this.props

    if (!dygraph) {
      return null
    }

    return (
      <div className="annotations-container">
        {annotations.map((a, i) =>
          <Annotation key={i} annotation={a} dygraph={dygraph} />
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
}

export default Annotations
