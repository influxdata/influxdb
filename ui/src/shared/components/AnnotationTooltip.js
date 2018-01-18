import React, {PropTypes} from 'react'

import {tooltipStyle} from 'src/shared/annotations/styles'

const AnnotationTooltip = ({
  annotation,
  onMouseLeave,
  annotationState: {isDragging, isMouseOver},
}) => {
  const humanTime = `${new Date(+annotation.time)}`
  const isVisible = isDragging || isMouseOver

  return (
    <div
      id={`tooltip-${annotation.id}`}
      onMouseLeave={onMouseLeave}
      style={tooltipStyle(isVisible)}
    >
      {!isDragging &&
        <span>
          {annotation.name}
        </span>}
      <span>
        {humanTime}
      </span>
    </div>
  )
}

const {func, shape} = PropTypes

AnnotationTooltip.propTypes = {
  annotation: shape({}).isRequired,
  onMouseLeave: func.isRequired,
  annotationState: shape({}),
}

export default AnnotationTooltip
