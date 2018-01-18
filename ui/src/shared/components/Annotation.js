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

  handleDrag = e => {
    if (!this.state.isDragging) {
      return
    }
    const {pageX} = e
    const {annotation, dygraph, onUpdateAnnotation} = this.props
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let time = dygraph.toDataXCoord(graphX)

    const minPercentChange = 0.5

    // check annotation.id for `-end`
    // if true:
    // updateAnnotation with duration instead of time
    if (
      Math.abs(
        dygraph.toPercentXCoord(time) - dygraph.toPercentXCoord(annotation.time)
      ) *
        100 <
      minPercentChange
    ) {
      return
    }

    if (time >= endX) {
      time = endX
    }

    if (time <= startX) {
      time = startX
    }

    onUpdateAnnotation({...annotation, time: `${time}`})

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

const {func, shape, string} = PropTypes

Annotation.propTypes = {
  annotation: shape({
    id: string.isRequired,
    time: string.isRequired,
    duration: string,
  }).isRequired,
  dygraph: shape({}).isRequired,
  onUpdateAnnotation: func.isRequired,
}

export default Annotation
