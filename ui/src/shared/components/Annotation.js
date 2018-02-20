import React, {Component, PropTypes} from 'react'

import AnnotationTooltip from 'src/shared/components/AnnotationTooltip'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'

import {ADDING, EDITING} from 'src/shared/annotations/helpers'
import * as schema from 'shared/schemas'

import {
  flagStyle,
  clickAreaStyle,
  annotationStyle,
} from 'src/shared/annotations/styles'

class Annotation extends Component {
  state = {
    isDragging: false,
    isMouseOver: false,
  }

  handleStartDrag = () => {
    const {mode} = this.props
    if (mode === ADDING || mode === null) {
      return
    }

    this.setState({isDragging: true})
  }

  handleStopDrag = () => {
    this.setState({isDragging: false})
  }

  handleMouseEnter = () => {
    this.setState({isMouseOver: true})
  }

  handleMouseLeave = e => {
    const {annotation} = this.props

    if (e.relatedTarget.id === `tooltip-${annotation.id}`) {
      return this.setState({isDragging: false})
    }
    this.setState({isDragging: false, isMouseOver: false})
  }

  handleDrag = e => {
    if (!this.state.isDragging) {
      return
    }

    const {pageX} = e
    const {annotation, dygraph, onUpdateAnnotation} = this.props
    const {startTime} = annotation
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let newTime = dygraph.toDataXCoord(graphX)
    const oldTime = +startTime

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

    onUpdateAnnotation({...annotation, startTime: `${newTime}`})

    e.preventDefault()
    e.stopPropagation()
  }

  renderPoint(
    annotation,
    dygraph,
    isMouseOver,
    isDragging,
    humanTime,
    isEditing
  ) {
    return (
      <div
        className="dygraph-annotation"
        style={annotationStyle(annotation, dygraph, isMouseOver, isDragging)}
        data-time-ms={annotation.startTime}
        data-time-local={humanTime}
      >
        <div
          style={clickAreaStyle(isDragging, isEditing)}
          onMouseMove={this.handleDrag}
          onMouseDown={this.handleStartDrag}
          onMouseUp={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={flagStyle(isMouseOver, isDragging, false, false)} />
        <AnnotationTooltip
          isEditing={isEditing}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
      </div>
    )
  }

  renderLeftMarker(
    annotation,
    dygraph,
    isMouseOver,
    isDragging,
    humanTime,
    isEditing
  ) {
    return (
      <div
        className="dygraph-annotation"
        style={annotationStyle(
          annotation.startTime,
          dygraph,
          isMouseOver,
          isDragging
        )}
        data-time-ms={annotation.startTime}
        data-time-local={humanTime}
      >
        <div
          style={clickAreaStyle(isDragging, isEditing)}
          onMouseMove={this.handleDrag}
          onMouseDown={this.handleStartDrag}
          onMouseUp={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={flagStyle(isMouseOver, isDragging, true, false)} />
        <AnnotationTooltip
          isEditing={isEditing}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
      </div>
    )
  }

  renderRightMarker(
    annotation,
    dygraph,
    isMouseOver,
    isDragging,
    humanTime,
    isEditing
  ) {
    return (
      <div
        className="dygraph-annotation"
        style={annotationStyle(
          annotation.endTime,
          dygraph,
          isMouseOver,
          isDragging
        )}
        data-time-ms={annotation.endTime}
        data-time-local={humanTime}
      >
        <div
          style={clickAreaStyle(isDragging, isEditing)}
          onMouseMove={this.handleDrag}
          onMouseDown={this.handleStartDrag}
          onMouseUp={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={flagStyle(isMouseOver, isDragging, true, true)} />
        <AnnotationTooltip
          isEditing={isEditing}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
      </div>
    )
  }

  renderSpan(
    annotation,
    dygraph,
    isMouseOver,
    isDragging,
    humanTime,
    isEditing
  ) {
    return (
      <div>
        {this.renderLeftMarker(
          annotation,
          dygraph,
          isMouseOver,
          isDragging,
          humanTime,
          isEditing
        )}
        {this.renderRightMarker(
          annotation,
          dygraph,
          isMouseOver,
          isDragging,
          humanTime,
          isEditing
        )}
        <AnnotationWindow
          key={annotation.id}
          annotation={annotation}
          dygraph={dygraph}
        />
      </div>
    )
  }

  render() {
    const {dygraph, annotation, mode} = this.props
    const {isDragging, isMouseOver} = this.state

    const humanTime = `${new Date(+annotation.startTime)}`

    const isEditing = mode === EDITING

    return (
      <div>
        {annotation.startTime === annotation.endTime
          ? this.renderPoint(
              annotation,
              dygraph,
              isMouseOver,
              isDragging,
              humanTime,
              isEditing
            )
          : this.renderSpan(
              annotation,
              dygraph,
              isMouseOver,
              isDragging,
              humanTime,
              isEditing
            )}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  onUpdateAnnotation: func.isRequired,
}

export default Annotation
