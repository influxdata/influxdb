import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import moment from 'moment'

import AnnotationInput from 'src/shared/components/AnnotationInput'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'

const TimeStamp = ({time}) =>
  <div className="annotation-tooltip--timestmap">
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
    this.props.updateAnnotation(this.state.annotation)
  }

  handleRejectUpdate = () => {
    this.setState({annotation: this.props.annotation})
  }

  handleDelete = () => {
    this.props.deleteAnnotationAsync(this.props.annotation)
  }

  render() {
    const {annotation} = this.state
    const {
      onMouseLeave,
      annotationState: {isDragging, isMouseOver},
      isEditing,
    } = this.props

    const tooltipClass =
      isDragging || isMouseOver
        ? 'annotation-tooltip'
        : 'annotation-tooltip hidden'

    return (
      <div
        id={`tooltip-${annotation.id}`}
        onMouseLeave={onMouseLeave}
        className={tooltipClass}
      >
        {isDragging
          ? <TimeStamp time={this.props.annotation.startTime} />
          : <div className="annotation-tooltip--items">
              {isEditing &&
                <button
                  className="annotation-tooltip--delete"
                  onClick={this.handleDelete}
                >
                  <span className="icon remove" />
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
              <TimeStamp time={this.props.annotation.startTime} />
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
  annotation: schema.annotation.isRequired,
  onMouseLeave: func.isRequired,
  annotationState: shape({}),
  deleteAnnotationAsync: func.isRequired,
  updateAnnotation: func.isRequired,
}

export default connect(null, {
  deleteAnnotationAsync: actions.deleteAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
})(AnnotationTooltip)
