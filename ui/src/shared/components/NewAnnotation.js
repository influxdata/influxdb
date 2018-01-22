import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import OnClickOutside from 'shared/components/OnClickOutside'

import {
  newAnnotationContainer,
  newAnnotationCrosshairStyle,
  newAnnotationTooltipStyle,
  newAnnotationFlagStyle,
  newAnnotationHelperStyle,
  newAnnotationTimestampStyle,
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
      onAddAnnotation,
      onAddingAnnotationSuccess,
      tempAnnotation,
      onMouseLeaveTempAnnotation,
      dygraph,
    } = this.props
    const {trueGraphX} = this.state

    if (this.state.mouseAction === 'dragging') {
      // time on mouse down
      const staticTime = dygraph.toDataXCoord(trueGraphX)
      // time on mouse up
      const draggingTime = Number(tempAnnotation.time)
      const duration = draggingTime - staticTime

      onAddAnnotation({
        ...tempAnnotation,
        time: `${staticTime}`,
        duration: `${duration}`,
      })
      onAddingAnnotationSuccess()

      return this.setState({
        isMouseOver: false,
        mouseAction: null,
        trueGraphX: null,
      })
    }

    onMouseLeaveTempAnnotation()
    onAddAnnotation(tempAnnotation)
    onAddingAnnotationSuccess()
    return this.setState({
      isMouseOver: false,
      mouseAction: null,
      trueGraphX: null,
    })
  }

  handleMouseMove = e => {
    const {isTempHovering} = this.props
    this.setState({mouseAction: 'dragging'})

    if (isTempHovering === false) {
      return
    }

    const {dygraph, tempAnnotation, onUpdateAnnotation} = this.props
    const wrapperRect = this.wrapper.getBoundingClientRect()
    const trueGraphX = e.pageX - wrapperRect.left

    const time = `${dygraph.toDataXCoord(trueGraphX)}`

    onUpdateAnnotation({...tempAnnotation, time})
  }

  handleMouseLeave = () => {
    this.setState({isMouseOver: false})
    this.props.onMouseLeaveTempAnnotation()
  }

  handleMouseDown = e => {
    const wrapperRect = this.wrapper.getBoundingClientRect()
    const trueGraphX = e.pageX - wrapperRect.left

    this.setState({mouseAction: null, trueGraphX})
  }

  handleClickOutside = () => {
    const {onDismissAddingAnnotation, isTempHovering} = this.props

    if (!isTempHovering) {
      onDismissAddingAnnotation()
    }
  }

  render() {
    const {dygraph, isTempHovering, tempAnnotation: {time}} = this.props
    const {isMouseOver, mouseAction} = this.state

    const timestamp = `${new Date(+time)}`

    const crosshairLeft = dygraph.toDomXCoord(time)
    const staticCrosshairLeft = this.state.trueGraphX

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
        {mouseAction === 'dragging' &&
          <div
            className="new-annotation--crosshair__static"
            style={newAnnotationCrosshairStyle(staticCrosshairLeft)}
          />}
        <div
          className="new-annotation--crosshair"
          style={newAnnotationCrosshairStyle(crosshairLeft)}
        >
          <div
            className="new-annotation--flag"
            style={newAnnotationFlagStyle}
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

const {bool, func, shape} = PropTypes

NewAnnotation.propTypes = {
  dygraph: shape({}).isRequired,
  isTempHovering: bool,
  tempAnnotation: shape({}).isRequired,
  onAddAnnotation: func.isRequired,
  onDismissAddingAnnotation: func.isRequired,
  onAddingAnnotationSuccess: func.isRequired,
  onUpdateAnnotation: func.isRequired,
  onMouseEnterTempAnnotation: func.isRequired,
  onMouseLeaveTempAnnotation: func.isRequired,
}

export default OnClickOutside(NewAnnotation)
