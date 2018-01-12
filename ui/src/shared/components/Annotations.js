import React, {PropTypes} from 'react'
import Annotation from 'src/shared/components/Annotation'

const style = {
  width: '100%',
  height: '100%',
  position: 'relative',
}

const Annotations = ({annotations, dygraph}) => {
  if (!dygraph) {
    return null
  }

  return (
    <div className="annotation-container" style={style}>
      {annotations.map((a, i) =>
        <Annotation key={i} annotation={a} dygraph={dygraph} />
      )}
    </div>
  )
}

const {arrayOf, shape} = PropTypes

Annotations.propTypes = {
  annotations: arrayOf(shape({})),
  dygraph: shape({}),
}

export default Annotations
