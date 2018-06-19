import React, {Component, MouseEvent} from 'react'
import classnames from 'classnames'
import {connect} from 'react-redux'
import uuid from 'uuid'

import OnClickOutside from 'src/shared/components/OnClickOutside'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'
import * as actions from 'src/shared/actions/annotations'

import {DYGRAPH_CONTAINER_XLABEL_MARGIN} from 'src/shared/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {AnnotationInterface, DygraphClass, Source} from 'src/types'

interface Props {
  dygraph: DygraphClass
  source: Source
  isTempHovering: boolean
  tempAnnotation: AnnotationInterface
  addAnnotationAsync: (url: string, a: AnnotationInterface) => void
  onDismissAddingAnnotation: () => void
  onAddingAnnotationSuccess: () => void
  onUpdateAnnotation: (a: AnnotationInterface) => void
  onMouseEnterTempAnnotation: () => void
  onMouseLeaveTempAnnotation: () => void
  staticLegendHeight: number
}
interface State {
  isMouseOver: boolean
  gatherMode: string
}

@ErrorHandling
class NewAnnotation extends Component<Props, State> {
  public wrapperRef: React.RefObject<HTMLDivElement>
  constructor(props: Props) {
    super(props)
    this.wrapperRef = React.createRef<HTMLDivElement>()
    this.state = {
      isMouseOver: false,
      gatherMode: 'startTime',
    }
  }

  public render() {
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
          ref={this.wrapperRef}
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

  public handleClickOutside = () => {
    const {onDismissAddingAnnotation, isTempHovering} = this.props
    if (!isTempHovering) {
      onDismissAddingAnnotation()
    }
  }

  private clampWithinGraphTimerange = (timestamp: number): number => {
    const [xRangeStart] = this.props.dygraph.xAxisRange()
    return Math.max(xRangeStart, timestamp)
  }

  private eventToTimestamp = ({
    pageX: pxBetweenMouseAndPage,
  }: MouseEvent<HTMLDivElement>): number => {
    const {
      left: pxBetweenGraphAndPage,
    } = this.wrapperRef.current.getBoundingClientRect()
    const graphXCoordinate = pxBetweenMouseAndPage - pxBetweenGraphAndPage
    const timestamp = this.props.dygraph.toDataXCoord(graphXCoordinate)
    const clamped = this.clampWithinGraphTimerange(timestamp)
    return clamped
  }

  private handleMouseDown = (e: MouseEvent<HTMLDivElement>) => {
    const startTime = this.eventToTimestamp(e)
    this.props.onUpdateAnnotation({...this.props.tempAnnotation, startTime})
    this.setState({gatherMode: 'endTime'})
  }

  private handleMouseMove = (e: MouseEvent<HTMLDivElement>) => {
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

  private handleMouseUp = (e: MouseEvent<HTMLDivElement>) => {
    const {
      tempAnnotation,
      onUpdateAnnotation,
      addAnnotationAsync,
      onAddingAnnotationSuccess,
      onMouseLeaveTempAnnotation,
      source,
    } = this.props
    const createUrl = source.links.annotations

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

  private handleMouseOver = (e: MouseEvent<HTMLDivElement>) => {
    this.setState({isMouseOver: true})
    this.handleMouseMove(e)
    this.props.onMouseEnterTempAnnotation()
  }

  private handleMouseLeave = () => {
    this.setState({isMouseOver: false})
    this.props.onMouseLeaveTempAnnotation()
  }

  private renderTimestamp(time: number): JSX.Element {
    const timestamp = `${new Date(time)}`

    return (
      <div className="new-annotation-tooltip">
        <span className="new-annotation-helper">Click or Drag to Annotate</span>
        <span className="new-annotation-timestamp">{timestamp}</span>
      </div>
    )
  }
}

const mdtp = {
  addAnnotationAsync: actions.addAnnotationAsync,
}

export default connect(null, mdtp)(OnClickOutside(NewAnnotation))
