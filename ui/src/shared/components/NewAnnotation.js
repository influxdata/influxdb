import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'
import uuid from 'node-uuid'

import OnClickOutside from 'shared/components/OnClickOutside'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'

import {
  circleFlagStyle,
  staticFlagStyle,
  draggingFlagStyle,
  newAnnotationContainer,
  newAnnotationCrosshairStyle,
  newAnnotationTooltipStyle,
  newAnnotationHelperStyle,
  newAnnotationTimestampStyle,
  newAnnotationWindowStyle,
} from 'src/shared/annotations/styles'

class NewAnnotation extends Component {
  state = {
    isMouseOver: false,
    mouseAction: null,
  }

  handleMouseOver = () => {
    this.setState({isMouseOver: true})
    this.props.onMouseEnterTempAnnotation()
  }

  handleMouseUp = () => {
    const {
      addAnnotation,
      onAddingAnnotationSuccess,
      tempAnnotation,
      onMouseLeaveTempAnnotation,
      dygraph,
    } = this.props
    const createUrl = this.context.source.links.annotations

    // time on mouse down
    const startTime = `${dygraph.toDataXCoord(this.state.trueGraphX)}`

    if (this.state.mouseAction === 'dragging') {
      // time on mouse up
      const endTime = tempAnnotation.startTime

      addAnnotation(createUrl, {
        ...tempAnnotation,
        startTime,
        endTime,
        text: 'hi',
        type: 'hi',
      })
      onAddingAnnotationSuccess()

      return this.setState({
        isMouseOver: false,
        mouseAction: null,
        trueGraphX: null,
      })
    }

    onAddingAnnotationSuccess()
    onMouseLeaveTempAnnotation()

    addAnnotation(createUrl, {
      ...tempAnnotation,
      id: uuid.v4(),
      startTime,
      endTime: '',
      text: 'hi',
      type: 'hi',
    })

    return this.setState({
      isMouseOver: false,
      mouseAction: null,
      trueGraphX: null,
    })
  }

  handleMouseMove = e => {
    const {isTempHovering} = this.props
    if (this.state.mouseAction === 'down') {
      this.setState({mouseAction: 'dragging'})
    }

    if (isTempHovering === false) {
      return
    }

    const {dygraph, tempAnnotation, onUpdateAnnotation} = this.props
    const wrapperRect = this.wrapper.getBoundingClientRect()
    const trueGraphX = e.pageX - wrapperRect.left

    const startTime = `${dygraph.toDataXCoord(trueGraphX)}`

    onUpdateAnnotation({...tempAnnotation, startTime})
  }

  handleMouseLeave = () => {
    this.setState({isMouseOver: false})
    this.props.onMouseLeaveTempAnnotation()
  }

  handleMouseDown = e => {
    const wrapperRect = this.wrapper.getBoundingClientRect()
    const trueGraphX = e.pageX - wrapperRect.left

    this.setState({mouseAction: 'down', trueGraphX})
  }

  handleClickOutside = () => {
    const {onDismissAddingAnnotation, isTempHovering} = this.props

    if (!isTempHovering) {
      onDismissAddingAnnotation()
    }
  }

  render() {
    const {dygraph, isTempHovering, tempAnnotation: {startTime}} = this.props
    const {isMouseOver, mouseAction} = this.state

    const timestamp = `${new Date(+startTime)}`

    const crosshairLeft = dygraph.toDomXCoord(startTime)
    const staticCrosshairLeft = this.state.trueGraphX

    const isDragging = mouseAction === 'dragging'

    return (
      <div
        className={classnames('new-annotation', {hover: isTempHovering})}
        ref={el => (this.wrapper = el)}
        onMouseMove={this.handleMouseMove}
        onMouseOver={this.handleMouseOver}
        onMouseLeave={this.handleMouseLeave}
        onMouseUp={this.handleMouseUp}
        onMouseDown={this.handleMouseDown}
        style={newAnnotationContainer}
      >
        {isDragging &&
          <div
            className="new-annotation--crosshair__static"
            style={newAnnotationCrosshairStyle(staticCrosshairLeft)}
          >
            <div style={isDragging ? staticFlagStyle : circleFlagStyle} />
          </div>}
        <div
          className="new-annotation--window"
          style={newAnnotationWindowStyle(
            isDragging,
            staticCrosshairLeft,
            crosshairLeft
          )}
        />
        <div
          className="new-annotation--crosshair"
          style={newAnnotationCrosshairStyle(crosshairLeft)}
        >
          <div
            className="new-annotation--flag"
            style={isDragging ? draggingFlagStyle : circleFlagStyle}
          />
          <div
            className="new-annotation--tooltip"
            style={newAnnotationTooltipStyle(isMouseOver)}
          >
            <span style={newAnnotationHelperStyle}>Click to Annotate</span>
            <span style={newAnnotationTimestampStyle}>
              {timestamp}
            </span>
          </div>
        </div>
      </div>
    )
  }
}

const {bool, func, shape, string} = PropTypes

NewAnnotation.contextTypes = {
  source: shape({
    links: shape({
      annotations: string,
    }),
  }),
}

NewAnnotation.propTypes = {
  dygraph: shape({}).isRequired,
  isTempHovering: bool,
  tempAnnotation: schema.annotation.isRequired,
  addAnnotation: func.isRequired,
  onDismissAddingAnnotation: func.isRequired,
  onAddingAnnotationSuccess: func.isRequired,
  onUpdateAnnotation: func.isRequired,
  onMouseEnterTempAnnotation: func.isRequired,
  onMouseLeaveTempAnnotation: func.isRequired,
}

const mdtp = dispatch => ({
  addAnnotation: bindActionCreators(actions.addAnnotationAsync, dispatch),
})

export default connect(null, mdtp)(OnClickOutside(NewAnnotation))
