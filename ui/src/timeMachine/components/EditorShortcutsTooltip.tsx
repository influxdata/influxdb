import React, {FC} from 'react'

import QuestionMarkTooltip from 'src/shared/components/question_mark_tooltip/QuestionMarkTooltip'

const EditorShortcutsTooltip: FC = () => {
  return (
    <QuestionMarkTooltip
      testID="editor-shortcuts"
      tipContent={
        <div className="editor-shortcuts">
          <h5>Shortcuts</h5>
          <dl className="editor-shortcuts--body">
            <dt>Ctl-/:</dt> <dd>Toggle comment for line or lines</dd>
            <dt>Ctl-Enter:</dt> <dd>Submit Script</dd>
          </dl>
        </div>
      }
    />
  )
}

export default EditorShortcutsTooltip
