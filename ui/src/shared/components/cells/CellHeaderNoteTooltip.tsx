// Libraries
import React, {SFC, CSSProperties} from 'react'
import {createPortal} from 'react-dom'

// Constants
import {NOTES_PORTAL_ID} from 'src/portals/NotesPortal'

import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

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
        <MarkdownRenderer text={note} />
      </div>
    </div>
  )

  return createPortal(content, document.querySelector(`#${NOTES_PORTAL_ID}`))
}

export default CellHeaderNoteTooltip
