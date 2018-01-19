import React, {Component, PropTypes} from 'react'

import AnnotationInput from 'src/shared/components/AnnotationInput'

import {
  tooltipStyle,
  tooltipItemsStyle,
  tooltipTimestampStyle,
} from 'src/shared/annotations/styles'

const TimeStamp = ({time}) =>
  <div style={tooltipTimestampStyle}>
    {`${new Date(+time)}`}
  </div>

class AnnotationTooltip extends Component {
  state = {
    annotation: this.props.annotation,
  }

  handleChangeInput = key => value => {
    const {annotation} = this.state
    const newAnnotation = {...annotation, [key]: value}

    this.setState({annotation: newAnnotation})
  }

  handleConfirmUpdate = () => {
    this.props.onConfirmUpdate(this.state.annotation)
  }

  handleRejectUpdate = () => {
    this.setState({annotation: this.props.annotation})
  }

  render() {
    const {annotation} = this.state
    const {
      onMouseLeave,
      annotationState,
      annotationState: {isDragging},
    } = this.props

    return (
      <div
        id={`tooltip-${annotation.id}`}
        onMouseLeave={onMouseLeave}
        style={tooltipStyle(annotationState)}
      >
        {isDragging
          ? <TimeStamp time={this.props.annotation.time} />
          : <div style={tooltipItemsStyle}>
              <button
                className="btn btn-sm btn-danger btn-square"
                onClick={this.props.onDelete}
              >
                <span className="icon trash" />
              </button>
              <AnnotationInput
                value={annotation.name}
                onChangeInput={this.handleChangeInput('name')}
                onConfirmUpdate={this.handleConfirmUpdate}
                onRejectUpdate={this.handleRejectUpdate}
              />
              <AnnotationInput
                value={annotation.text}
                onChangeInput={this.handleChangeInput('text')}
                onConfirmUpdate={this.handleConfirmUpdate}
                onRejectUpdate={this.handleRejectUpdate}
              />
              <TimeStamp time={this.props.annotation.time} />
            </div>}
      </div>
    )
  }
}

const {func, shape, string} = PropTypes

TimeStamp.propTypes = {
  time: string.isRequired,
}
AnnotationTooltip.propTypes = {
  annotation: shape({}).isRequired,
  onMouseLeave: func.isRequired,
  annotationState: shape({}),
  onConfirmUpdate: func.isRequired,
  onDelete: func.isRequired,
}

export default AnnotationTooltip
