import React, {PropTypes} from 'react'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'
import * as schema from 'shared/schemas'

const style = {
  position: 'absolute',
  width: 'calc(100% - 32px)',
  height: 'calc(100% - 16px)',
  top: '8px',
  zIndex: '50',
  overflow: 'hidden',
}

const AnnotationWindows = ({annotations, dygraph}) => {
  if (!dygraph) {
    return null
  }

  return (
    <div className="annotation-windows-container" style={style}>
      {annotations.map((a, i) => {
        return a.endTime
          ? <AnnotationWindow key={i} annotation={a} dygraph={dygraph} />
          : null
      })}
    </div>
  )
}

const {arrayOf, shape} = PropTypes

AnnotationWindows.propTypes = {
  annotations: arrayOf(schema.annotation),
  dygraph: shape({}),
}

export default AnnotationWindows
