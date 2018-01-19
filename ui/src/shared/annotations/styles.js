import {NEUTRALS} from 'src/shared/constants/InfluxColors'

// Styles for all things Annotations
const annotationColor = '255,255,255'
const annotationDragColor = '0,255,0'
const zIndexWindow = '1'
const zIndexAnnotation = '3'
const zIndexAnnotationActive = '4'

export const flagStyle = (mouseOver, dragging) => {
  const style = {
    position: 'absolute',
    top: '-3px',
    left: '-2px',
    width: '6px',
    height: '6px',
    backgroundColor: `rgb(${annotationColor})`,
    borderRadius: '50%',
    zIndex: '2',
    transition: 'background-color 0.25s ease, transform 0.25s ease',
  }

  if (dragging) {
    return {
      ...style,
      transform: 'scale(1.5,1.5)',
      backgroundColor: `rgb(${annotationDragColor})`,
    }
  }
  if (mouseOver) {
    return {...style, transform: 'scale(1.5,1.5)'}
  }

  return style
}

export const clickAreaStyle = dragging => {
  const style = {
    position: 'absolute',
    top: '-8px',
    left: '-7px',
    width: '16px',
    height: '16px',
    zIndex: '4',
    cursor: 'pointer',
  }

  if (dragging) {
    return {
      ...style,
      width: '10000px',
      left: '-5000px',
      height: '10000px',
      top: '-5000px',
    }
  }

  return style
}

export const tooltipStyle = annotationState => {
  const {isDragging, isMouseOver} = annotationState
  const isVisible = isDragging || isMouseOver

  return {
    position: 'absolute',
    bottom: 'calc(100% + 8px)',
    left: '50%',
    transform: 'translateX(-50%)',
    backgroundColor: NEUTRALS[0],
    zIndex: '3',
    color: NEUTRALS[20],
    fontSize: '14px',
    fontWeight: '600',
    padding: isDragging ? '6px' : '12px',
    borderRadius: '4px',
    whiteSpace: 'nowrap',
    userSelect: 'none',
    display: isVisible ? 'flex' : 'none',
  }
}

export const tooltipItemsStyle = {
  display: 'flex',
  flexDirection: 'column',
}
export const tooltipTimestampStyle = {display: 'block'}
export const tooltipInputContainer = {marginBottom: '4px'}
export const tooltipFormStyle = {
  display: 'inline-flex',
  alignItems: 'center',
  flexWrap: 'nowrap',
  width: '100%',
}
export const tooltipInputButton = {marginLeft: '2px'}
export const tooltipInput = {flex: '1 0 0'}

export const annotationStyle = ({time}, dygraph, isMouseOver, isDragging) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()
  let visibility = 'visible'

  if (time < startX || time > endX) {
    visibility = 'hidden'
  }

  const containerLeftPadding = 16
  const left = `${dygraph.toDomXCoord(time) + containerLeftPadding}px`
  const width = 2

  return {
    left,
    position: 'absolute',
    top: '8px',
    backgroundColor: `rgb(${isDragging
      ? annotationDragColor
      : annotationColor})`,
    height: 'calc(100% - 36px)',
    width: `${width}px`,
    transition: 'backgroundColor 0.25s ease',
    transform: `translateX(-${width / 2}px)`, // translate should always be half with width to horizontally center the annotation pole
    visibility,
    zIndex:
      isDragging || isMouseOver ? zIndexAnnotationActive : zIndexAnnotation,
  }
}

export const annotationWindowStyle = (annotation, dygraph) => {
  // TODO: export and test this function
  const [startX, endX] = dygraph.xAxisRange()
  const containerLeftPadding = 16
  const windowEnd = Number(annotation.time) + Number(annotation.duration)

  let windowStartXCoord = dygraph.toDomXCoord(annotation.time)
  let windowEndXCoord = dygraph.toDomXCoord(windowEnd)
  let visibility = 'visible'

  if (annotation.time < startX) {
    windowStartXCoord = dygraph.toDomXCoord(startX)
  }

  if (windowEnd > endX) {
    windowEndXCoord = dygraph.toDomXCoord(endX)
  }

  if (windowEnd < startX || annotation.time > endX) {
    visibility = 'hidden'
  }

  const windowWidth = windowEndXCoord - windowStartXCoord
  const isDurationNegative = windowWidth < 0
  const foo = isDurationNegative ? windowWidth : 0

  const left = `${windowStartXCoord + containerLeftPadding + foo}px`
  const width = `${Math.abs(windowWidth)}px`

  const gradientStartColor = `rgba(${annotationColor},0.3)`
  const gradientEndColor = `rgba(${annotationColor},0)`

  return {
    left,
    position: 'absolute',
    top: '8px',
    background: `linear-gradient(to bottom, ${gradientStartColor} 0%,${gradientEndColor} 100%)`,
    height: 'calc(100% - 36px)',
    borderTop: `2px solid rgb(${annotationColor})`,
    width,
    zIndex: zIndexWindow,
    visibility,
  }
}
