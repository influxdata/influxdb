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
  }
  handleMouseOver = () => {
    this.setState({isMouseOver: true})
    this.props.onMouseEnterTempAnnotation()
  }

  handleMouseMove = e => {
    const {isTempHovering} = this.props

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

  handleClick = () => {
    const {
      onAddAnnotation,
      onAddingAnnotationSuccess,
      tempAnnotation,
      onMouseLeaveTempAnnotation,
    } = this.props

    this.setState({isMouseOver: false})
    onMouseLeaveTempAnnotation()
    onAddAnnotation(tempAnnotation)
    onAddingAnnotationSuccess()
  }

  handleClickOutside = () => {
    const {onDismissAddingAnnotation, isTempHovering} = this.props

    if (!isTempHovering) {
      onDismissAddingAnnotation()
    }
  }

  render() {
    const {dygraph, isTempHovering, tempAnnotation: {time}} = this.props
    const {isMouseOver} = this.state

    const timestamp = `${new Date(+time)}`

    const crosshairLeft = dygraph.toDomXCoord(time)

    return (
      <div
        className={classnames('new-annotation', {hover: isTempHovering})}
        ref={el => (this.wrapper = el)}
        onMouseMove={this.handleMouseMove}
        onMouseOver={this.handleMouseOver}
        onMouseLeave={this.handleMouseLeave}
        onClick={this.handleClick}
        style={newAnnotationContainer}
      >
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
