// Libraries
import React, {FC, useContext} from 'react'

// Types
import {MarkdownMode} from './'

// Components
import MarkdownModeToggle from './MarkdownModeToggle'
import MarkdownMonacoEditor from 'src/shared/components/MarkdownMonacoEditor'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'
import {ClickOutside} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'
import {PipeProp} from 'src/notebooks'

const MarkdownPanel: FC<PipeProp> = ({Context}) => {
  const {data, update} = useContext(PipeContext)
  const handleToggleMode = (mode: MarkdownMode): void => {
    update({mode})
  }

  const handleClickOutside = (): void => {
    update({mode: 'preview'})
  }

  const handlePreviewClick = (): void => {
    update({mode: 'edit'})
  }

  const controls = (
    <MarkdownModeToggle mode={data.mode} onToggleMode={handleToggleMode} />
  )

  const handleChange = (text: string): void => {
    update({text})
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
