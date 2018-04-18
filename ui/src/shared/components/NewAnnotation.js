import React, {Component} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import {connect} from 'react-redux'
import uuid from 'uuid'

import OnClickOutside from 'shared/components/OnClickOutside'
import AnnotationWindow from 'shared/components/AnnotationWindow'
import * as schema from 'shared/schemas'
import * as actions from 'shared/actions/annotations'

import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'shared/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class NewAnnotation extends Component {
  state = {
    isMouseOver: false,
    gatherMode: 'startTime',
  }

  clampWithinGraphTimerange = timestamp => {
    const [xRangeStart] = this.props.dygraph.xAxisRange()
    return Math.max(xRangeStart, timestamp)
  }

  eventToTimestamp = ({pageX: pxBetweenMouseAndPage}) => {
    const {left: pxBetweenGraphAndPage} = this.wrapper.getBoundingClientRect()
    const graphXCoordinate = pxBetweenMouseAndPage - pxBetweenGraphAndPage
    const timestamp = this.props.dygraph.toDataXCoord(graphXCoordinate)
    const clamped = this.clampWithinGraphTimerange(timestamp)
    return `${clamped}`
  }

  handleMouseDown = e => {
    const startTime = this.eventToTimestamp(e)
    this.props.onUpdateAnnotation({...this.props.tempAnnotation, startTime})
    this.setState({gatherMode: 'endTime'})
  }

  handleMouseMove = e => {
    if (this.props.isTempHovering === false) {
      return
    }

    const {tempAnnotation, onUpdateAnnotation} = this.props
    const newTime = this.eventToTimestamp(e)

    if (this.state.gatherMode === 'startTime') {
      onUpdateAnnotation({
        ...tempAnnotation,
        startTime: newTime,
        endTime: newTime,
      })
    } else {
      onUpdateAnnotation({...tempAnnotation, endTime: newTime})
    }
  }

  handleMouseUp = e => {
    const {
      tempAnnotation,
      onUpdateAnnotation,
      addAnnotationAsync,
      onAddingAnnotationSuccess,
      onMouseLeaveTempAnnotation,
    } = this.props
    const createUrl = this.context.source.links.annotations

    const upTime = this.eventToTimestamp(e)
    const downTime = tempAnnotation.startTime
    const [startTime, endTime] = [downTime, upTime].sort()
    const newAnnotation = {...tempAnnotation, startTime, endTime}

    onUpdateAnnotation(newAnnotation)
    addAnnotationAsync(createUrl, {...newAnnotation, id: uuid.v4()})

    onAddingAnnotationSuccess()
    onMouseLeaveTempAnnotation()

    this.setState({
      isMouseOver: false,
      gatherMode: 'startTime',
    })
  }

  handleMouseOver = e => {
    this.setState({isMouseOver: true})
    this.handleMouseMove(e)
    this.props.onMouseEnterTempAnnotation()
  }

  handleMouseLeave = () => {
    this.setState({isMouseOver: false})
    this.props.onMouseLeaveTempAnnotation()
  }

  handleClickOutside = () => {
    const {onDismissAddingAnnotation, isTempHovering} = this.props

    if (!isTempHovering) {
      onDismissAddingAnnotation()
    }
  }

  renderTimestamp(time) {
    const timestamp = `${new Date(+time)}`

    return (
      <div className="new-annotation-tooltip">
        <span className="new-annotation-helper">Click or Drag to Annotate</span>
        <span className="new-annotation-timestamp">{timestamp}</span>
      </div>
    )
  }

  render() {
    const {
      dygraph,
      isTempHovering,
      tempAnnotation,
      tempAnnotation: {startTime, endTime},
      staticLegendHeight,
    } = this.props
    const {isMouseOver} = this.state

    const crosshairOne = Math.max(-1000, dygraph.toDomXCoord(startTime))
    const crosshairTwo = dygraph.toDomXCoord(endTime)
    const crosshairHeight = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_XLABEL_MARGIN}px)`

    const isDragging = startTime !== endTime
    const flagOneClass =
      crosshairOne < crosshairTwo
        ? 'annotation-span--left-flag dragging'
        : 'annotation-span--right-flag dragging'
    const flagTwoClass =
      crosshairOne < crosshairTwo
        ? 'annotation-span--right-flag dragging'
        : 'annotation-span--left-flag dragging'
    const pointFlagClass = 'annotation-point--flag__dragging'

    return (
      <div>
        {isDragging && (
          <AnnotationWindow
            annotation={tempAnnotation}
            dygraph={dygraph}
            active={true}
            staticLegendHeight={staticLegendHeight}
          />
        )}
        <div
          className={classnames('new-annotation', {
            hover: isTempHovering,
          })}
          ref={el => (this.wrapper = el)}
          onMouseMove={this.handleMouseMove}
          onMouseOver={this.handleMouseOver}
          onMouseLeave={this.handleMouseLeave}
          onMouseUp={this.handleMouseUp}
          onMouseDown={this.handleMouseDown}
        >
          {isDragging && (
            <div
              className="new-annotation--crosshair"
              style={{left: crosshairTwo, height: crosshairHeight}}
            >
              {isMouseOver &&
                isDragging &&
                this.renderTimestamp(tempAnnotation.endTime)}
              <div className={flagTwoClass} />
            </div>
          )}
          <div
            className="new-annotation--crosshair"
            style={{left: crosshairOne, height: crosshairHeight}}
          >
            {isMouseOver &&
              !isDragging &&
              this.renderTimestamp(tempAnnotation.startTime)}
            <div className={isDragging ? flagOneClass : pointFlagClass} />
          </div>
        </div>
      </div>
    )
  }
}

const {bool, func, number, shape, string} = PropTypes

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
  addAnnotationAsync: func.isRequired,
  onDismissAddingAnnotation: func.isRequired,
  onAddingAnnotationSuccess: func.isRequired,
  onUpdateAnnotation: func.isRequired,
  onMouseEnterTempAnnotation: func.isRequired,
  onMouseLeaveTempAnnotation: func.isRequired,
  staticLegendHeight: number,
}

const mdtp = {
  addAnnotationAsync: actions.addAnnotationAsync,
}

export default connect(null, mdtp)(OnClickOutside(NewAnnotation))
