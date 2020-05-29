// Libraries
import React, {FC} from 'react'

// Components
import {QuestionMarkTooltip} from '@influxdata/clockface'

const EditorShortcutsTooltip: FC = () => {
  return (
    <QuestionMarkTooltip
      testID="editor-shortcuts"
      tooltipContents={
        <div className="editor-shortcuts">
          <h5>Shortcuts</h5>
          <dl className="editor-shortcuts--body">
            <dt>[Ctl or ⌘] + /:</dt>
            <dd>Toggle comment for line or lines</dd>
            <dt>[Ctl or ⌘] + [Enter]:</dt>
            <dd>Submit Script</dd>
          </dl>
        </div>
      }
    />
  )
}

export default EditorShortcutsTooltip
