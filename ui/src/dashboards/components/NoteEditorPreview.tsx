import React, {SFC, MouseEvent} from 'react'

import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

interface Props {
  note: string
  scrollTop: number
  onScroll: (e: MouseEvent) => void
}

const cloudImageRenderer = (): any =>
  "We don't support images in markdown for security purposes"

const NoteEditorPreview: SFC<Props> = props => {
  return (
    <div className="note-editor--preview">
      <FancyScrollbar
        className="note-editor--preview-scroll"
        scrollTop={props.scrollTop}
        setScrollTop={props.onScroll}
      >
        <div className="note-editor--markdown-container">
          <MarkdownRenderer
            text={props.note}
            className="markdown-format"
            cloudRenderers={{
              image: cloudImageRenderer,
              imageReference: cloudImageRenderer,
            }}
          />
        </div>
      </FancyScrollbar>
    </div>
  )
}

export default NoteEditorPreview
