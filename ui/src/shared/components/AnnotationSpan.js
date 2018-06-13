import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import {
  DYGRAPH_CONTAINER_H_MARGIN,
  DYGRAPH_CONTAINER_V_MARGIN,
  DYGRAPH_CONTAINER_XLABEL_MARGIN,
} from 'shared/constants'
import {ANNOTATION_MIN_DELTA, EDITING} from 'shared/annotations/helpers'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'
import AnnotationTooltip from 'shared/components/AnnotationTooltip'
import AnnotationWindow from 'shared/components/AnnotationWindow'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class AnnotationSpan extends React.Component {
  state = {
    isDragging: null,
    isMouseOver: null,
  }

  handleMouseEnter = direction => () => {
    this.setState({isMouseOver: direction})
  }

  handleMouseLeave = e => {
    const {annotation} = this.props

    if (e.relatedTarget.id === `tooltip-${annotation.id}`) {
      return this.setState({isDragging: null})
    }
    this.setState({isMouseOver: null})
  }

  handleDragStart = direction => () => {
    this.setState({isDragging: direction})
  }

  handleDragEnd = () => {
    const {annotation, updateAnnotationAsync} = this.props
    const [startTime, endTime] = [
      annotation.startTime,
      annotation.endTime,
    ].sort()
    const newAnnotation = {
      ...annotation,
      startTime,
      endTime,
    }
    updateAnnotationAsync(newAnnotation)

    this.setState({isDragging: null})
  }

  handleDrag = timeProp => e => {
    if (this.props.mode !== EDITING) {
      return
    }

    const {pageX} = e
    const {annotation, dygraph, updateAnnotation} = this.props

    if (pageX === 0) {
      return
    }

    const oldTime = +annotation[timeProp]
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let newTime = dygraph.toDataXCoord(graphX)

    if (
      Math.abs(
        dygraph.toPercentXCoord(newTime) - dygraph.toPercentXCoord(oldTime)
      ) *
        100 <
      ANNOTATION_MIN_DELTA
    ) {
      return
    }

    if (newTime >= endX) {
      newTime = endX
    }

    if (newTime <= startX) {
      newTime = startX
    }

    updateAnnotation({...annotation, [timeProp]: `${newTime}`})
    e.preventDefault()
    e.stopPropagation()
  }

  renderLeftMarker(startTime, dygraph) {
    const isEditing = this.props.mode === EDITING
    const {isDragging, isMouseOver} = this.state
    const {annotation, staticLegendHeight} = this.props

    const flagClass = isDragging
      ? 'annotation-span--left-flag dragging'
      : 'annotation-span--left-flag'
    const markerClass = isDragging ? 'annotation dragging' : 'annotation'
    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const leftBound = dygraph.xAxisRange()[0]
    if (startTime < leftBound) {
      return null
    }
    const showTooltip = isDragging === 'left' || isMouseOver === 'left'

    const markerStyles = {
      left: `${dygraph.toDomXCoord(startTime) + DYGRAPH_CONTAINER_H_MARGIN}px`,
      height: `calc(100% - ${staticLegendHeight +
        DYGRAPH_CONTAINER_XLABEL_MARGIN +
        DYGRAPH_CONTAINER_V_MARGIN * 2}px)`,
    }

    return (
      <div className={markerClass} style={markerStyles}>
        {showTooltip && (
          <AnnotationTooltip
            isEditing={isEditing}
            timestamp={annotation.startTime}
            annotation={annotation}
            onMouseLeave={this.handleMouseLeave}
            annotationState={this.state}
          />
        )}
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag('startTime')}
          onDragStart={this.handleDragStart('left')}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter('left')}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
      </div>
    )
  }

  renderRightMarker(endTime, dygraph) {
    const isEditing = this.props.mode === EDITING
    const {isDragging, isMouseOver} = this.state
    const {annotation, staticLegendHeight} = this.props

    const flagClass = isDragging
      ? 'annotation-span--right-flag dragging'
      : 'annotation-span--right-flag'
    const markerClass = isDragging ? 'annotation dragging' : 'annotation'
    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const rightBound = dygraph.xAxisRange()[1]
    if (rightBound < endTime) {
      return null
    }
    const showTooltip = isDragging === 'right' || isMouseOver === 'right'

    const markerStyles = {
      left: `${dygraph.toDomXCoord(endTime) + DYGRAPH_CONTAINER_H_MARGIN}px`,
      height: `calc(100% - ${staticLegendHeight +
        DYGRAPH_CONTAINER_XLABEL_MARGIN +
        DYGRAPH_CONTAINER_V_MARGIN * 2}px)`,
    }

    return (
      <div className={markerClass} style={markerStyles}>
        {showTooltip && (
          <AnnotationTooltip
            isEditing={isEditing}
            timestamp={annotation.endTime}
            annotation={annotation}
            onMouseLeave={this.handleMouseLeave}
            annotationState={this.state}
          />
        )}
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag('endTime')}
          onDragStart={this.handleDragStart('right')}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter('right')}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
      </div>
    )
  }

  render() {
    const {annotation, dygraph, staticLegendHeight} = this.props
    const {isDragging} = this.state

    return (
      <div>
        <AnnotationWindow
          annotation={annotation}
          dygraph={dygraph}
          active={!!isDragging}
          staticLegendHeight={staticLegendHeight}
        />
        {this.renderLeftMarker(annotation.startTime, dygraph)}
        {this.renderRightMarker(annotation.endTime, dygraph)}
      </div>
    )
  }
}

const {arrayOf, func, number, shape, string} = PropTypes

AnnotationSpan.defaultProps = {
  staticLegendHeight: 0,
}

AnnotationSpan.propTypes = {
  annotation: schema.annotation.isRequired,
  mode: string.isRequired,
  dygraph: shape({}).isRequired,
  staticLegendHeight: number,
  updateAnnotationAsync: func.isRequired,
  updateAnnotation: func.isRequired,
  xAxisRange: arrayOf(number),
}

const mapDispatchToProps = {
  updateAnnotationAsync: actions.updateAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
}

export default connect(null, mapDispatchToProps)(AnnotationSpan)
