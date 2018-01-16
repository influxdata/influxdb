import React, {PropTypes} from 'react'
import Annotation from 'src/shared/components/Annotation'

const style = {
  position: 'absolute',
  width: '0',
  height: 'calc(100% - 16px)',
  top: '8px',
  zIndex: '150',
}

const Annotations = ({annotations, dygraph}) => {
  if (!dygraph) {
    return null
  }

  return (
    <div className="annotations-container" style={style}>
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
