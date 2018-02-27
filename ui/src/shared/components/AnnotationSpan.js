import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

import {DYGRAPH_CONTAINER_MARGIN} from 'shared/constants'
import {ANNOTATION_MIN_DELTA, EDITING} from 'shared/annotations/helpers'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'
import AnnotationTooltip from 'shared/components/AnnotationTooltip'
import AnnotationWindow from 'shared/components/AnnotationWindow'

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
    const {annotation} = this.props

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

    const left = dygraph.toDomXCoord(startTime) + DYGRAPH_CONTAINER_MARGIN

    return (
      <div className={markerClass} style={{left: `${left}px`}}>
        {showTooltip &&
          <AnnotationTooltip
            isEditing={isEditing}
            timestamp={annotation.startTime}
            annotation={annotation}
            onMouseLeave={this.handleMouseLeave}
            annotationState={this.state}
          />}
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
    const humanTime = `${new Date(+endTime)}`
    const {isDragging, isMouseOver} = this.state
    const {annotation} = this.props

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

    const left = `${dygraph.toDomXCoord(endTime) + DYGRAPH_CONTAINER_MARGIN}px`

    return (
      <div
        className={markerClass}
        style={{left}}
        data-time-ms={endTime}
        data-time-local={humanTime}
      >
        {showTooltip &&
          <AnnotationTooltip
            isEditing={isEditing}
            timestamp={annotation.endTime}
            annotation={annotation}
            onMouseLeave={this.handleMouseLeave}
            annotationState={this.state}
          />}
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

AnnotationSpan.propTypes = {
  annotation: schema.annotation.isRequired,
  mode: PropTypes.string.isRequired,
  dygraph: PropTypes.shape({}).isRequired,
  staticLegendHeight: PropTypes.number.isRequired,
  updateAnnotationAsync: PropTypes.func.isRequired,
  updateAnnotation: PropTypes.func.isRequired,
}

const mdtp = {
  updateAnnotationAsync: actions.updateAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
}

export default connect(null, mdtp)(AnnotationSpan)
