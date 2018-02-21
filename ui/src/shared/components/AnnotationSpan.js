import React, {PropTypes} from 'react'
import {connect} from 'react-redux'

import {EDITING} from 'shared/annotations/helpers'
import * as schema from 'shared/schemas'
import * as style from 'shared/annotations/styles'
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
    this.setState({isDragging: false, isMouseOver: false})
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
          onDragStart={this.handleDragStart}
          onDragEnd={this.handleDragEnd}
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
          onDragStart={this.handleDragStart}
          onDragEnd={this.handleDragEnd}
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
  updateAnnotationAsync: PropTypes.func.isRequired,
  updateAnnotation: PropTypes.func.isRequired,
}

const mdtp = {
  updateAnnotationAsync: actions.updateAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
}

export default connect(null, mdtp)(AnnotationSpan)
