import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

import {EDITING} from 'shared/annotations/helpers'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'
import AnnotationTooltip from 'shared/components/AnnotationTooltip'
import AnnotationWindow from 'shared/components/AnnotationWindow'

class AnnotationSpan extends React.Component {
  state = {
    isDragging: false,
    isMouseOver: false,
  }

  handleMouseEnter = () => {
    this.setState({isMouseOver: true})
  }

  handleMouseLeave = e => {
    const {annotation} = this.props

    if (e.relatedTarget.id === `tooltip-${annotation.id}`) {
      return this.setState({isDragging: false})
    }
    this.setState({isMouseOver: false})
  }

  handleDragStart = () => {
    this.setState({isDragging: true})
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

    this.setState({isDragging: false})
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

    const minPercentChange = 0.5

    if (
      Math.abs(
        dygraph.toPercentXCoord(newTime) - dygraph.toPercentXCoord(oldTime)
      ) *
        100 <
      minPercentChange
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
    const humanTime = `${new Date(+startTime)}`
    const {isDragging} = this.state
    const {annotation} = this.props

    const flagClass = isDragging
      ? 'annotation-span--left-flag dragging'
      : 'annotation-span--left-flag'

    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const leftBound = dygraph.xAxisRange()[0]
    if (startTime < leftBound) {
      return null
    }

    const left = dygraph.toDomXCoord(startTime) + 16

    return (
      <div
        className="annotation"
        style={{left: `${left}px`}}
        data-time-ms={startTime}
        data-time-local={humanTime}
      >
        <AnnotationTooltip
          isEditing={isEditing}
          timestamp={annotation.startTime}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag('startTime')}
          onDragStart={this.handleDragStart}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
      </div>
    )
  }

  renderRightMarker(endTime, dygraph) {
    const isEditing = this.props.mode === EDITING
    const humanTime = `${new Date(+endTime)}`
    const {isDragging} = this.state
    const {annotation} = this.props

    const flagClass = isDragging
      ? 'annotation-span--right-flag dragging'
      : 'annotation-span--right-flag'
    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const left = `${dygraph.toDomXCoord(endTime) + 16}px`

    return (
      <div
        className="annotation"
        style={{left}}
        data-time-ms={endTime}
        data-time-local={humanTime}
      >
        <AnnotationTooltip
          isEditing={isEditing}
          timestamp={annotation.endTime}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag('endTime')}
          onDragStart={this.handleDragStart}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
      </div>
    )
  }

  render() {
    const {annotation, dygraph} = this.props

    return (
      <div>
        <AnnotationWindow annotation={annotation} dygraph={dygraph} />
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
  updateAnnotationAsync: PropTypes.func.isRequired,
  updateAnnotation: PropTypes.func.isRequired,
}

const mdtp = {
  updateAnnotationAsync: actions.updateAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
}

export default connect(null, mdtp)(AnnotationSpan)
