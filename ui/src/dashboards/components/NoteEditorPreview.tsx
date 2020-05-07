import React, {SFC, MouseEvent} from 'react'

import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'
import { DapperScrollbars } from '@influxdata/clockface'

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
      <DapperScrollbars
        className="note-editor--preview-scroll"
        scrollTop={props.scrollTop}
        onScroll={props.onScroll}
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
      </DapperScrollbars>
    </div>
  )
}

export default NoteEditorPreview
