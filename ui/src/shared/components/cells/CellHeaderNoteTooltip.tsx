// Libraries
import React, {SFC, CSSProperties} from 'react'
import {createPortal} from 'react-dom'
import ReactMarkdown from 'react-markdown'

// Styles
import 'src/shared/components/cells/CellHeaderNoteTooltip.scss'

interface Props {
  note: string
  containerStyle: CSSProperties
  maxWidth: number
  maxHeight: number
}

const CellHeaderNoteTooltip: SFC<Props> = props => {
  const {note, containerStyle, maxWidth, maxHeight} = props

  const style = {
    maxWidth: `${maxWidth}px`,
    maxHeight: `${maxHeight}px`,
  }

  const content = (
    <div className="cell-header-note-tooltip" style={containerStyle}>
      <div
        className="cell-header-note-tooltip--content markdown-format"
        style={style}
      >
        <ReactMarkdown source={note} />
      </div>
    </div>
  )

  return createPortal(
    content,
    document.querySelector('.cell-header-note-tooltip-container')
  )
}

export default CellHeaderNoteTooltip
