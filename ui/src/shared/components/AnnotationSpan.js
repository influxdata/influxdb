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

  handleStartDrag = () => {
    console.log('??')
    this.setState({isDragging: true})
  }

  handleStopDrag = () => {
    this.setState({isDragging: false})
  }

  handleDrag = timeProp => e => {
    if (this.props.mode !== EDITING) {
      return
    }

    const {pageX} = e
    const {annotation, dygraph, onUpdateAnnotation} = this.props

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

    onUpdateAnnotation({...annotation, [timeProp]: `${newTime}`})

    e.preventDefault()
    e.stopPropagation()
  }

  renderLeftMarker(startTime, dygraph) {
    const isEditing = this.props.mode === EDITING
    const humanTime = `${new Date(+startTime)}`
    const {isMouseOver, isDragging} = this.state

    return (
      <div
        className="dygraph-annotation"
        style={style.annotation(startTime, dygraph, isMouseOver, isDragging)}
        data-time-ms={startTime}
        data-time-local={humanTime}
      >
        <div
          style={style.clickArea(isEditing)}
          draggable={true}
          onDrag={this.handleDrag('startTime')}
          onDragStart={this.handleStartDrag}
          onDragEnd={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={style.flag(isMouseOver, isDragging, true, false)} />
      </div>
    )
  }

  renderRightMarker(endTime, dygraph) {
    const isEditing = this.props.mode === EDITING
    const humanTime = `${new Date(+endTime)}`
    const {isMouseOver, isDragging} = this.state

    return (
      <div
        className="dygraph-annotation"
        style={style.annotation(endTime, dygraph, isMouseOver, isDragging)}
        data-time-ms={endTime}
        data-time-local={humanTime}
      >
        <div
          style={style.clickArea(isEditing)}
          draggable={true}
          onDrag={this.handleDrag('endTime')}
          onDragStart={this.handleStartDrag}
          onDragEnd={this.handleStopDrag}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div style={style.flag(isMouseOver, isDragging, true, true)} />
      </div>
    )
  }

  render() {
    const {annotation, dygraph} = this.props
    const isEditing = this.props.mode === EDITING

    return (
      <div>
        <AnnotationWindow
          key={annotation.id}
          annotation={annotation}
          dygraph={dygraph}
        />
        <AnnotationTooltip
          isEditing={isEditing}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
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
  onUpdateAnnotation: PropTypes.func.isRequired,
}

export default AnnotationSpan
