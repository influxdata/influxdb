import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import moment from 'moment'
import classnames from 'classnames'

import AnnotationInput from 'src/shared/components/AnnotationInput'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'
import {ErrorHandling} from 'src/shared/decorators/errors'

const TimeStamp = ({time}) => (
  <div className="annotation-tooltip--timestamp">
    {`${moment(+time).format('YYYY/MM/DD HH:mm:ss.SS')}`}
  </div>
)

@ErrorHandling
class AnnotationTooltip extends Component {
  state = {
    annotation: this.props.annotation,
  }

  componentWillReceiveProps = ({annotation}) => {
    this.setState({annotation})
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
      span,
    } = this.props

    const tooltipClass = classnames('annotation-tooltip', {
      hidden: !(isDragging || isMouseOver),
      'annotation-span-tooltip': !!span,
    })

    return (
      <div
        id={`tooltip-${annotation.id}`}
        onMouseLeave={onMouseLeave}
        className={tooltipClass}
        style={
          span
            ? {left: `${span.tooltipLeft}px`, minWidth: `${span.spanWidth}px`}
            : {}
        }
      >
        {isDragging ? (
          <TimeStamp time={timestamp} />
        ) : (
          <div className="annotation-tooltip--items">
            {isEditing ? (
              <div>
                <AnnotationInput
                  value={annotation.text}
                  onChangeInput={this.handleChangeInput('text')}
                  onConfirmUpdate={this.handleConfirmUpdate}
                  onRejectUpdate={this.handleRejectUpdate}
                />
                <button
                  className="annotation-tooltip--delete"
                  onClick={this.handleDelete}
                  title="Delete this Annotation"
                >
                  <span className="icon trash" />
                </button>
              </div>
            ) : (
              <div>{annotation.text}</div>
            )}
            <TimeStamp time={timestamp} />
          </div>
        )}
      </div>
    )
  }
}

const {bool, func, number, shape, string} = PropTypes

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
  span: shape({
    spanCenter: number.isRequired,
    spanWidth: number.isRequired,
  }),
}

export default connect(null, {
  deleteAnnotationAsync: actions.deleteAnnotationAsync,
  updateAnnotationAsync: actions.updateAnnotationAsync,
})(AnnotationTooltip)
