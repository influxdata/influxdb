import React, {Component, PropTypes} from 'react'

import AnnotationTooltip from 'src/shared/components/AnnotationTooltip'

import {TEMP_ANNOTATION} from 'src/shared/annotations/helpers'

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
    const {time, duration} = annotation
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let newTime = dygraph.toDataXCoord(graphX)
    const oldTime = +time

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

      const newDuration = newTime - oldTime + Number(startAnnotation.duration)

      this.counter = this.counter + 1
      return onUpdateAnnotation({
        ...startAnnotation,
        duration: `${newDuration}`,
      })
    }

    if (duration) {
      const differenceInTimes = oldTime - newTime
      const newDuration = Number(duration) + differenceInTimes

      return onUpdateAnnotation({
        ...annotation,
        time: `${newTime}`,
        duration: `${newDuration}`,
      })
    }

    onUpdateAnnotation({...annotation, time: `${newTime}`})

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
    this.props.onDeleteAnnotation(this.props.annotation)
  }

  render() {
    const {dygraph, annotation} = this.props
    const {isDragging, isMouseOver} = this.state

    const humanTime = `${new Date(+annotation.time)}`
    const hasDuration = !!annotation.duration

    if (annotation.id === TEMP_ANNOTATION.id) {
      return null
    }

    return (
      <div
        className="dygraph-annotation"
        style={annotationStyle(annotation, dygraph, isMouseOver, isDragging)}
        data-time-ms={annotation.time}
        data-time-local={humanTime}
      >
        <div
          style={clickAreaStyle(isDragging)}
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
  annotations: arrayOf(shape({})),
  annotation: shape({
    id: string.isRequired,
    time: string.isRequired,
    duration: string,
  }).isRequired,
  dygraph: shape({}).isRequired,
  onUpdateAnnotation: func.isRequired,
  onDeleteAnnotation: func.isRequired,
}

export default Annotation
