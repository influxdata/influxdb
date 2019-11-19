// Libraries
import React, {FunctionComponent, MouseEvent} from 'react'

// Utils
import {useDragBehavior, DragEvent} from 'src/shared/utils/useDragBehavior'

// Types
import {CheckStatusLevel} from 'src/types'

interface Props {
  level: CheckStatusLevel
  y: number
  onDrag: (e: DragEvent) => void
  onMouseUp: (e: MouseEvent<HTMLDivElement>) => void
}

const ThresholdMarker: FunctionComponent<Props> = ({
  level,
  y,
  onDrag,
  onMouseUp,
}) => {
  const dragTargetProps = useDragBehavior(onDrag)
  const levelClass = `threshold-marker--${level.toLowerCase()}`
  const style = {top: `${y}px`}

  return (
    <>
      <div className={`threshold-marker--line ${levelClass}`} style={style} />
      <div
        className={`threshold-marker--handle ${levelClass}`}
        style={style}
        {...dragTargetProps}
        onMouseUp={onMouseUp}
      />
    </>
  )
}

export default ThresholdMarker
