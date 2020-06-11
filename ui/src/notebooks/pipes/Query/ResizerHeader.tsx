// Libraries
import React, {FC, RefObject} from 'react'
import classnames from 'classnames'

// Components
import {Icon, IconFont} from '@influxdata/clockface'

// Types
import {ResultsVisibility} from 'src/notebooks/pipes/Query'

interface Props {
  visibility: ResultsVisibility
  onUpdateVisibility: (visibility: ResultsVisibility) => void
  onStartDrag: () => void
  resizingEnabled: boolean
  dragHandleRef: RefObject<HTMLDivElement>
}

const ResultsHeader: FC<Props> = ({
  visibility,
  onUpdateVisibility,
  onStartDrag,
  resizingEnabled,
  dragHandleRef,
}) => {
  const glyph = visibility === 'visible' ? IconFont.EyeOpen : IconFont.EyeClosed
  const className = classnames('notebook-raw-data--header', {
    [`notebook-raw-data--header__${visibility}`]: resizingEnabled && visibility,
  })

  if (!resizingEnabled) {
    return (
      <div className={className}>
        <Icon glyph={IconFont.Zap} className="notebook-raw-data--vis-toggle" />
      </div>
    )
  }

  const handleToggleVisibility = (): void => {
    if (visibility === 'visible') {
      onUpdateVisibility('hidden')
    } else {
      onUpdateVisibility('visible')
    }
  }

  return (
    <div className={className}>
      <div onClick={handleToggleVisibility}>
        <Icon className="notebook-raw-data--vis-toggle" glyph={glyph} />
      </div>
      <div
        className="notebook-raw-data--drag-handle"
        onMouseDown={onStartDrag}
        ref={dragHandleRef}
        title="Drag to resize results table"
      >
        <div className="notebook-raw-data--drag-icon" />
        <div className="notebook-raw-data--drag-icon" />
        <div className="notebook-raw-data--drag-icon" />
      </div>
    </div>
  )
}

export default ResultsHeader
