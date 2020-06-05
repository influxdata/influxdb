// Libraries
import React, {FC} from 'react'

// Types
import {PipeProp} from 'src/notebooks'
import {MarkdownMode} from './'

// Components
import MarkdownModeToggle from './MarkdownModeToggle'
import MarkdownMonacoEditor from 'src/shared/components/MarkdownMonacoEditor'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'
import {ClickOutside} from '@influxdata/clockface'

const MarkdownPanel: FC<PipeProp> = ({data, Context, onUpdate}) => {
  const handleToggleMode = (mode: MarkdownMode): void => {
    onUpdate({mode})
  }

  const handleClickOutside = (): void => {
    onUpdate({mode: 'preview'})
  }

  const handlePreviewClick = (): void => {
    onUpdate({mode: 'edit'})
  }

  const controls = (
    <MarkdownModeToggle mode={data.mode} onToggleMode={handleToggleMode} />
  )

  const handleChange = (text: string): void => {
    onUpdate({text})
  }

  let panelContents = (
    <ClickOutside onClickOutside={handleClickOutside}>
      <MarkdownMonacoEditor
        script={data.text}
        onChangeScript={handleChange}
        autogrow
      />
    </ClickOutside>
  )

  if (data.mode === 'preview') {
    panelContents = (
      <div
        className="notebook-panel--markdown markdown-format"
        onClick={handlePreviewClick}
      >
        <MarkdownRenderer text={data.text} />
      </div>
    )
  }

  return <Context controls={controls}>{panelContents}</Context>
}

export default MarkdownPanel
