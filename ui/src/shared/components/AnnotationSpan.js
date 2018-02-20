import React, {PropTypes} from 'react'

import {EDITING} from 'shared/annotations/helpers'
import * as schema from 'shared/schemas'
import * as style from 'shared/annotations/styles'
import AnnotationTooltip from 'shared/components/AnnotationTooltip'
import AnnotationWindow from 'shared/components/AnnotationWindow'

class AnnotationSpan extends React.Component {
  state = {
    isDragging: false,
    isMouseOver: false,
  }

  handleStartDrag = () => {
    const {mode} = this.props
    if (mode !== EDITING) {
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

  renderLeftMarker(annotation, dygraph) {
    const isEditing = this.props.mode === EDITING
    const humanTime = `${new Date(+annotation.startTime)}`
    const {isMouseOver, isDragging} = this.state

    return (
      <div
        className="dygraph-annotation"
        style={style.annotation(
          annotation.startTime,
          dygraph,
          isMouseOver,
          isDragging
        )}
        data-time-ms={annotation.startTime}
        data-time-local={humanTime}
      >
        <div
          style={style.clickArea(isDragging, isEditing)}
          onMouseMove={this.handleDrag}
          onMouseDown={this.handleStartDrag}
          onMouseUp={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={style.flag(isMouseOver, isDragging, true, false)} />
        <AnnotationTooltip
          isEditing={isEditing}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
      </div>
    )
  }

  renderRightMarker(annotation, dygraph) {
    const isEditing = this.props.mode === EDITING
    const humanTime = `${new Date(+annotation.startTime)}`
    const {isMouseOver, isDragging} = this.state

    return (
      <div
        className="dygraph-annotation"
        style={style.annotation(
          annotation.endTime,
          dygraph,
          isMouseOver,
          isDragging
        )}
        data-time-ms={annotation.endTime}
        data-time-local={humanTime}
      >
        <div
          style={style.clickArea(isDragging, isEditing)}
          onMouseMove={this.handleDrag}
          onMouseDown={this.handleStartDrag}
          onMouseUp={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={style.flag(isMouseOver, isDragging, true, true)} />
      </div>
    )
  }

  render() {
    const {annotation, dygraph} = this.props

    return (
      <div>
        {this.renderLeftMarker(annotation, dygraph)}
        {this.renderRightMarker(annotation, dygraph)}
        <AnnotationWindow
          key={annotation.id}
          annotation={annotation}
          dygraph={dygraph}
        />
      </div>
    )
  }
}

AnnotationSpan.propTypes = {
  annotation: schema.annotation.isRequired,
  mode: PropTypes.string.isRequired,
  dygraph: PropTypes.shape({}).isRequired,
  onUpdateAnnotation: PropTypes.func.isRequired,
}

export default AnnotationSpan
