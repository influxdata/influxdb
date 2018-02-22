import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import moment from 'moment'

import AnnotationInput from 'src/shared/components/AnnotationInput'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'

const TimeStamp = ({time}) =>
  <div className="annotation-tooltip--timestamp">
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
    this.props.updateAnnotationAsync(this.state.annotation)
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
      timestamp,
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
          ? <TimeStamp time={timestamp} />
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
                    value={annotation.text}
                    onChangeInput={this.handleChangeInput('text')}
                    onConfirmUpdate={this.handleConfirmUpdate}
                    onRejectUpdate={this.handleRejectUpdate}
                  />
                : <div>
                    {annotation.text}
                  </div>}
              <TimeStamp time={timestamp} />
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
  timestamp: string,
  onMouseLeave: func.isRequired,
  annotationState: shape({}),
  deleteAnnotationAsync: func.isRequired,
  updateAnnotationAsync: func.isRequired,
}

export default connect(null, {
  deleteAnnotationAsync: actions.deleteAnnotationAsync,
  updateAnnotationAsync: actions.updateAnnotationAsync,
})(AnnotationTooltip)
