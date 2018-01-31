import React, {Component, PropTypes} from 'react'
import moment from 'moment'

import AnnotationInput from 'src/shared/components/AnnotationInput'

import {
  tooltipStyle,
  tooltipItemsStyle,
  tooltipTimestampStyle,
} from 'src/shared/annotations/styles'

const TimeStamp = ({time}) =>
  <div style={tooltipTimestampStyle}>
    {`${moment(+time).format('YYYY/DD/MM HH:mm:ss.SS')}`}
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
      isEditing,
    } = this.props

    return (
      <div
        id={`tooltip-${annotation.id}`}
        onMouseLeave={onMouseLeave}
        style={tooltipStyle(annotationState)}
        className="annotation-tooltip"
      >
        {isDragging
          ? <TimeStamp time={this.props.annotation.time} />
          : <div style={tooltipItemsStyle}>
              {isEditing &&
                <button
                  className="btn btn-sm btn-danger btn-square"
                  onClick={this.props.onDelete}
                >
                  <span className="icon trash" />
                </button>}
              {isEditing
                ? <AnnotationInput
                    value={annotation.name}
                    onChangeInput={this.handleChangeInput('name')}
                    onConfirmUpdate={this.handleConfirmUpdate}
                    onRejectUpdate={this.handleRejectUpdate}
                  />
                : <div>
                    {annotation.name}
                  </div>}
              <TimeStamp time={this.props.annotation.time} />
            </div>}
      </div>
    )
  }
}

const {bool, func, shape, string} = PropTypes

TimeStamp.propTypes = {
  time: string.isRequired,
}
AnnotationTooltip.propTypes = {
  isEditing: bool,
  annotation: shape({}).isRequired,
  onMouseLeave: func.isRequired,
  annotationState: shape({}),
  onConfirmUpdate: func.isRequired,
  onDelete: func.isRequired,
}

export default AnnotationTooltip
