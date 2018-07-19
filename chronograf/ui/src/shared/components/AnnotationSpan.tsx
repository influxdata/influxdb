import React, {Component, MouseEvent, DragEvent} from 'react'
import {connect} from 'react-redux'

import {
  DYGRAPH_CONTAINER_H_MARGIN,
  DYGRAPH_CONTAINER_V_MARGIN,
  DYGRAPH_CONTAINER_XLABEL_MARGIN,
} from 'src/shared/constants'

import * as actions from 'src/shared/actions/annotations'
import {ANNOTATION_MIN_DELTA, EDITING} from 'src/shared/annotations/helpers'
import AnnotationTooltip from 'src/shared/components/AnnotationTooltip'
import AnnotationWindow from 'src/shared/components/AnnotationWindow'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AnnotationInterface, DygraphClass} from 'src/types'

interface State {
  isMouseOver: string
  isDragging: string
}

interface Props {
  annotation: AnnotationInterface
  mode: string
  dygraph: DygraphClass
  staticLegendHeight: number
  updateAnnotation: (a: AnnotationInterface) => void
  updateAnnotationAsync: (a: AnnotationInterface) => void
  xAxisRange: [number, number]
}

@ErrorHandling
class AnnotationSpan extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    staticLegendHeight: 0,
  }

  public state: State = {
    isDragging: null,
    isMouseOver: null,
  }

  public render() {
    const {annotation, dygraph, staticLegendHeight} = this.props
    const {isDragging} = this.state

    return (
      <div>
        <AnnotationWindow
          annotation={annotation}
          dygraph={dygraph}
          active={!!isDragging}
          staticLegendHeight={staticLegendHeight}
        />
        {this.renderLeftMarker(annotation.startTime, dygraph)}
        {this.renderRightMarker(annotation.endTime, dygraph)}
      </div>
    )
  }

  private handleMouseEnter = (direction: string) => () => {
    this.setState({isMouseOver: direction})
  }

  private handleMouseLeave = (e: MouseEvent<HTMLDivElement>) => {
    const {annotation} = this.props
    if (e.relatedTarget instanceof Element) {
      if (e.relatedTarget.id === `tooltip-${annotation.id}`) {
        return this.setState({isDragging: null})
      }
    }
    this.setState({isMouseOver: null})
  }

  private handleDragStart = (direction: string) => () => {
    this.setState({isDragging: direction})
  }

  private handleDragEnd = () => {
    const {annotation, updateAnnotationAsync} = this.props
    const [startTime, endTime] = [
      annotation.startTime,
      annotation.endTime,
    ].sort()
    const newAnnotation = {
      ...annotation,
      startTime,
      endTime,
    }
    updateAnnotationAsync(newAnnotation)

    this.setState({isDragging: null})
  }

  private handleDrag = (timeProp: string) => (e: DragEvent<HTMLDivElement>) => {
    if (this.props.mode !== EDITING) {
      return
    }

    const {pageX} = e
    const {annotation, dygraph, updateAnnotation} = this.props

    if (pageX === 0) {
      return
    }

    const oldTime = +annotation[timeProp]
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let newTime = dygraph.toDataXCoord(graphX)

    if (
      Math.abs(
        dygraph.toPercentXCoord(newTime) - dygraph.toPercentXCoord(oldTime)
      ) *
        100 <
      ANNOTATION_MIN_DELTA
    ) {
      return
    }

    if (newTime >= endX) {
      newTime = endX
    }

    if (newTime <= startX) {
      newTime = startX
    }

    updateAnnotation({...annotation, [timeProp]: `${newTime}`})
    e.preventDefault()
    e.stopPropagation()
  }

  private renderLeftMarker(
    startTime: number,
    dygraph: DygraphClass
  ): JSX.Element {
    const isEditing = this.props.mode === EDITING
    const {isDragging, isMouseOver} = this.state
    const {annotation, staticLegendHeight} = this.props

    const flagClass = isDragging
      ? 'annotation-span--left-flag dragging'
      : 'annotation-span--left-flag'
    const markerClass = isDragging ? 'annotation dragging' : 'annotation'
    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const leftBound = dygraph.xAxisRange()[0]
    if (startTime < leftBound) {
      return null
    }
    const showTooltip = isDragging === 'left' || isMouseOver === 'left'

    const markerStyles = {
      left: `${dygraph.toDomXCoord(startTime) + DYGRAPH_CONTAINER_H_MARGIN}px`,
      height: `calc(100% - ${staticLegendHeight +
        DYGRAPH_CONTAINER_XLABEL_MARGIN +
        DYGRAPH_CONTAINER_V_MARGIN * 2}px)`,
    }

    return (
      <div className={markerClass} style={markerStyles}>
        {showTooltip && (
          <AnnotationTooltip
            isEditing={isEditing}
            timestamp={annotation.startTime}
            annotation={annotation}
            onMouseLeave={this.handleMouseLeave}
            annotationState={this.state}
          />
        )}
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag('startTime')}
          onDragStart={this.handleDragStart('left')}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter('left')}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
      </div>
    )
  }

  private renderRightMarker(
    endTime: number,
    dygraph: DygraphClass
  ): JSX.Element {
    const isEditing = this.props.mode === EDITING
    const {isDragging, isMouseOver} = this.state
    const {annotation, staticLegendHeight} = this.props

    const flagClass = isDragging
      ? 'annotation-span--right-flag dragging'
      : 'annotation-span--right-flag'
    const markerClass = isDragging ? 'annotation dragging' : 'annotation'
    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const rightBound = dygraph.xAxisRange()[1]
    if (rightBound < endTime) {
      return null
    }
    const showTooltip = isDragging === 'right' || isMouseOver === 'right'

    const markerStyles = {
      left: `${dygraph.toDomXCoord(endTime) + DYGRAPH_CONTAINER_H_MARGIN}px`,
      height: `calc(100% - ${staticLegendHeight +
        DYGRAPH_CONTAINER_XLABEL_MARGIN +
        DYGRAPH_CONTAINER_V_MARGIN * 2}px)`,
    }

    return (
      <div className={markerClass} style={markerStyles}>
        {showTooltip && (
          <AnnotationTooltip
            isEditing={isEditing}
            timestamp={annotation.endTime}
            annotation={annotation}
            onMouseLeave={this.handleMouseLeave}
            annotationState={this.state}
          />
        )}
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag('endTime')}
          onDragStart={this.handleDragStart('right')}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter('right')}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
      </div>
    )
  }
}

const mapDispatchToProps = {
  updateAnnotationAsync: actions.updateAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
}

export default connect(null, mapDispatchToProps)(AnnotationSpan)
