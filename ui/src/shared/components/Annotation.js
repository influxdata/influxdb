import React, {Component, PropTypes} from 'react'

import {
  annotationStyle,
  flagStyle,
  clickAreaStyle,
  timeIndicatorStyle,
} from 'src/shared/annotations/styles'

class Annotation extends Component {
  state = {
    isDragging: false,
    mouseOver: false,
  }

  handleStartDrag = () => {
    this.setState({isDragging: true})
  }

  handleStopDrag = () => {
    this.setState({isDragging: false})
  }

  handleMouseEnter = () => {
    this.setState({mouseOver: true})
  }

  handleMouseLeave = () => {
    this.setState({isDragging: false, mouseOver: false})
  }

  counter = 0

  handleDrag = e => {
    if (!this.state.isDragging) {
      return
    }

    const {pageX} = e
    const {annotation, annotations, dygraph, onUpdateAnnotation} = this.props
    const {id, time} = annotation
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let newTime = dygraph.toDataXCoord(graphX)

    const minPercentChange = 0.5

    if (
      Math.abs(
        dygraph.toPercentXCoord(newTime) - dygraph.toPercentXCoord(time)
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

    const idAppendage = '-end'
    const isEndpoint =
      id.substring(id.length - idAppendage.length) === idAppendage

    if (isEndpoint) {
      const startID = id.substring(0, id.length - idAppendage.length)
      const startAnnotation = annotations.find(a => a.id === startID)
      if (!startAnnotation) {
        return console.error('Start annotation does not exist')
      }

      const {duration} = startAnnotation
      const newDuration = Number(newTime) - Number(time) + Number(duration)

      this.counter = this.counter + 1
      return onUpdateAnnotation({
        ...startAnnotation,
        duration: `${newDuration}`,
      })
    }

    onUpdateAnnotation({...annotation, time: `${newTime}`})

    e.preventDefault()
    e.stopPropagation()
  }

  render() {
    const {annotation, dygraph} = this.props
    const {isDragging, mouseOver} = this.state

    const humanTime = `${new Date(+annotation.time)}`

    return (
      <div
        className="dygraph-annotation"
        style={annotationStyle(annotation, dygraph, isDragging)}
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
        <div style={flagStyle(mouseOver, isDragging)} />
        {isDragging
          ? <div style={timeIndicatorStyle}>
              {humanTime}
            </div>
          : null}
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
}

export default Annotation
