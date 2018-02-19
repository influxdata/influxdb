import React, {Component, PropTypes} from 'react'

import AnnotationTooltip from 'src/shared/components/AnnotationTooltip'

import {ADDING, EDITING, TEMP_ANNOTATION} from 'src/shared/annotations/helpers'
import * as schema from 'shared/schemas'

import {
  flagStyle,
  clickAreaStyle,
  annotationStyle,
} from 'src/shared/annotations/styles'

const idAppendage = '-end'

class Annotation extends Component {
  state = {
    isDragging: false,
    isMouseOver: false,
  }

  isEndpoint = () => {
    const {annotation: {id}} = this.props

    return id.substring(id.length - idAppendage.length) === idAppendage
  }

  getStartID = () => {
    const {annotation: {id}} = this.props

    return id.substring(0, id.length - idAppendage.length)
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
    const {annotation, annotations, dygraph, onUpdateAnnotation} = this.props
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

    if (this.isEndpoint()) {
      const startAnnotation = annotations.find(a => a.id === this.getStartID())
      if (!startAnnotation) {
        return console.error('Start annotation does not exist')
      }

      this.counter = this.counter + 1
      return onUpdateAnnotation({
        ...startAnnotation,
        endTime: newTime,
      })
    }

    onUpdateAnnotation({...annotation, startTime: `${newTime}`})

    e.preventDefault()
    e.stopPropagation()
  }

  handleConfirmUpdate = annotation => {
    const {onUpdateAnnotation} = this.props

    if (this.isEndpoint()) {
      const id = this.getStartID()
      return onUpdateAnnotation({...annotation, id})
    }

    onUpdateAnnotation(annotation)
  }

  handleDeleteAnnotation = () => {
    const {annotation, annotations, onDeleteAnnotation} = this.props

    if (this.isEndpoint()) {
      const startAnnotation = annotations.find(a => a.id === this.getStartID())

      return onDeleteAnnotation(startAnnotation)
    }

    return onDeleteAnnotation(annotation)
  }

  render() {
    const {dygraph, annotation, mode} = this.props
    const {isDragging, isMouseOver} = this.state

    const humanTime = `${new Date(+annotation.startTime)}`
    const hasDuration = annotation.starTime !== annotation.endTime

    if (annotation.id === TEMP_ANNOTATION.id) {
      return null
    }

    const isEditing = mode === EDITING

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
        <div
          style={flagStyle(
            isMouseOver,
            isDragging,
            hasDuration,
            this.isEndpoint()
          )}
        />
        <AnnotationTooltip
          isEditing={isEditing}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
          onConfirmUpdate={this.handleConfirmUpdate}
          onDelete={this.handleDeleteAnnotation}
        />
      </div>
    )
  }
}

const {arrayOf, func, shape, string} = PropTypes

Annotation.propTypes = {
  mode: string,
  annotations: arrayOf(schema.annotation).isRequired,
  annotation: schema.annotation.isRequired,
  dygraph: shape({}).isRequired,
  onUpdateAnnotation: func.isRequired,
  onDeleteAnnotation: func.isRequired,
}

export default Annotation
