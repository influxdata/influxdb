// Libraries
import React, {FC} from 'react'

// Types
import {PipeProp} from 'src/notebooks'
import {MarkdownMode} from './'

// Components
import MarkdownModeToggle from './MarkdownModeToggle'
import MarkdownPanelEditor from './MarkdownPanelEditor'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'

const MarkdownPanel: FC<PipeProp> = ({data, Context, onUpdate}) => {
  const handleToggleMode = (mode: MarkdownMode): void => {
    onUpdate({mode})
  }

  const controls = (
    <MarkdownModeToggle mode={data.mode} onToggleMode={handleToggleMode} />
  )

  const handleChange = (text: string): void => {
    onUpdate({text})
  }

  let panelContents = (
    <MarkdownPanelEditor text={data.text} onChange={handleChange} />
  )

  if (data.mode === 'preview') {
    panelContents = (
      <div className="notebook-panel--markdown markdown-format">
        <MarkdownRenderer text={data.text} />
      </div>
    )
  }

  return <Context controls={controls}>{panelContents}</Context>
}

export default MarkdownPanel
