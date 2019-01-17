import React, {SFC} from 'react'
import ReactMarkdown from 'react-markdown'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

interface Props {
  note: string
}

const NoteEditorPreview: SFC<Props> = props => {
  return (
    <div className="note-editor--preview">
      <FancyScrollbar className="note-editor--preview-scroll">
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
