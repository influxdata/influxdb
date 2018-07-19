import React, {Component, MouseEvent, DragEvent} from 'react'
import {connect} from 'react-redux'

import {
  DYGRAPH_CONTAINER_H_MARGIN,
  DYGRAPH_CONTAINER_V_MARGIN,
  DYGRAPH_CONTAINER_XLABEL_MARGIN,
} from 'src/shared/constants'
import {ANNOTATION_MIN_DELTA, EDITING} from 'src/shared/annotations/helpers'
import * as actions from 'src/shared/actions/annotations'
import AnnotationTooltip from 'src/shared/components/AnnotationTooltip'
import {ErrorHandling} from 'src/shared/decorators/errors'

import {AnnotationInterface, DygraphClass} from 'src/types'

interface State {
  isMouseOver: boolean
  isDragging: boolean
}

interface Props {
  annotation: AnnotationInterface
  mode: string
  xAxisRange: [number, number]
  dygraph: DygraphClass
  updateAnnotation: (a: AnnotationInterface) => void
  updateAnnotationAsync: (a: AnnotationInterface) => void
  staticLegendHeight: number
}

@ErrorHandling
class AnnotationPoint extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    staticLegendHeight: 0,
  }

  public state = {
    isMouseOver: false,
    isDragging: false,
  }

  public render() {
    const {annotation, mode, dygraph, staticLegendHeight} = this.props
    const {isDragging} = this.state

    const isEditing = mode === EDITING

    const flagClass = isDragging
      ? 'annotation-point--flag__dragging'
      : 'annotation-point--flag'

    const markerClass = isDragging ? 'annotation dragging' : 'annotation'

    const clickClass = isEditing
      ? 'annotation--click-area editing'
      : 'annotation--click-area'

    const markerStyles = {
      left: `${dygraph.toDomXCoord(Number(annotation.startTime)) +
        DYGRAPH_CONTAINER_H_MARGIN}px`,
      height: `calc(100% - ${staticLegendHeight +
        DYGRAPH_CONTAINER_XLABEL_MARGIN +
        DYGRAPH_CONTAINER_V_MARGIN * 2}px)`,
    }

    return (
      <div className={markerClass} style={markerStyles}>
        <div
          className={clickClass}
          draggable={true}
          onDrag={this.handleDrag}
          onDragStart={this.handleDragStart}
          onDragEnd={this.handleDragEnd}
          onMouseEnter={this.handleMouseEnter}
          onMouseLeave={this.handleMouseLeave}
        />
        <div className={flagClass} />
        <AnnotationTooltip
          isEditing={isEditing}
          timestamp={annotation.startTime}
          annotation={annotation}
          onMouseLeave={this.handleMouseLeave}
          annotationState={this.state}
        />
      </div>
    )
  }

  private handleMouseEnter = () => {
    this.setState({isMouseOver: true})
  }

  private handleMouseLeave = (e: MouseEvent<HTMLDivElement>) => {
    const {annotation} = this.props
    if (e.relatedTarget instanceof Element) {
      if (e.relatedTarget.id === `tooltip-${annotation.id}`) {
        return this.setState({isDragging: false})
      }
    }
    this.setState({isMouseOver: false})
  }

  private handleDragStart = () => {
    this.setState({isDragging: true})
  }

  private handleDragEnd = () => {
    const {annotation, updateAnnotationAsync} = this.props
    updateAnnotationAsync(annotation)
    this.setState({isDragging: false})
  }

  private handleDrag = (e: DragEvent<HTMLDivElement>) => {
    if (this.props.mode !== EDITING) {
      return
    }

    const {pageX} = e
    const {annotation, dygraph, updateAnnotation} = this.props

    if (pageX === 0) {
      return
    }

    const {startTime} = annotation
    const {left} = dygraph.graphDiv.getBoundingClientRect()
    const [startX, endX] = dygraph.xAxisRange()

    const graphX = pageX - left
    let newTime = dygraph.toDataXCoord(graphX)
    const oldTime = +startTime

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

    updateAnnotation({
      ...annotation,
      startTime: newTime,
      endTime: newTime,
    })

    e.preventDefault()
    e.stopPropagation()
  }
}

AnnotationPoint.defaultProps = {
  staticLegendHeight: 0,
}

const mdtp = {
  updateAnnotationAsync: actions.updateAnnotationAsync,
  updateAnnotation: actions.updateAnnotation,
}

export default connect(null, mdtp)(AnnotationPoint)
