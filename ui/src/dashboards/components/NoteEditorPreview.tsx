import React, {SFC, MouseEvent} from 'react'
import ReactMarkdown from 'react-markdown'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

interface Props {
  note: string
  scrollTop: number
  onScroll: (e: MouseEvent) => void
}

const NoteEditorPreview: SFC<Props> = props => {
  return (
    <div className="note-editor--preview">
      <FancyScrollbar
        className="note-editor--preview-scroll"
        scrollTop={props.scrollTop}
        setScrollTop={props.onScroll}
      >
        <div className="note-editor--markdown-container">
          <ReactMarkdown
            source={props.note}
            escapeHtml={true}
            className="markdown-format"
          />
        </div>
      </FancyScrollbar>
    </div>
  )
}

export default NoteEditorPreview
