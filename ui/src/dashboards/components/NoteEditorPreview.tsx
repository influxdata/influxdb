import React, {SFC} from 'react'
import ReactMarkdown from 'react-markdown'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

interface Props {
  note: string
}

const NoteEditorPreview: SFC<Props> = props => {
  return (
    <div className="note-editor-preview markdown-format">
      <FancyScrollbar>
        <ReactMarkdown source={props.note} escapeHtml={true} />
      </FancyScrollbar>
    </div>
  )
}

export default NoteEditorPreview
