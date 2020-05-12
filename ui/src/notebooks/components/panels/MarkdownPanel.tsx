// Libraries
import React, {FC, useState, ChangeEvent} from 'react'

// Components
import NotebookPanel from 'src/notebooks/components/NotebookPanel'
import {MarkdownRenderer} from 'src/shared/components/views/MarkdownRenderer'
import {SelectGroup} from '@influxdata/clockface'

interface Props {
  contents: string
  title: string
  onChangeContents: (contents: string) => void
  onRemovePanel: () => void
}


const MarkdownPanel: FC<Props> = ({
  contents,
  title,
  onChangeContents,
  onRemovePanel,
}) => {
  const [mode, switchMode] = useState<'edit' | 'preview'>('preview')

  const controls = (
    <SelectGroup>
      <SelectGroup.Option onClick={switchMode} id="edit" value="edit" active={mode === 'edit'}>Edit</SelectGroup.Option>
      <SelectGroup.Option onClick={switchMode} id="preview" value="preview" active={mode === 'preview'}>Preview</SelectGroup.Option>
    </SelectGroup>
  )

  const handleTextareaChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    onChangeContents(e.target.value)
  }

  let body = (
    <div className="notebook-panel--markdown markdown-format"><MarkdownRenderer text={contents} /></div>
  )

  if (mode === 'edit') {
    body = (
      <textarea value={contents} onChange={handleTextareaChange} />
    )
  }

  return (
    <NotebookPanel
      id={title}
      title={title}
      onRemove={onRemovePanel}
      controlsRight={controls}
    >
      {body}
    </NotebookPanel>
  )
}


export default MarkdownPanel
