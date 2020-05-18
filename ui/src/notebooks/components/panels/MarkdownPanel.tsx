// Libraries
import React, {FC, useState, useRef, useEffect, ChangeEvent} from 'react'

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

type MarkdownMode = 'edit' | 'preview'

const MarkdownPanel: FC<Props> = ({
  contents,
  title,
  onChangeContents,
  onRemovePanel,
}) => {
  const previewRef = useRef<HTMLDivElement>(null)
  const [mode, switchMode] = useState<MarkdownMode>('preview')
  const [previewHeight, setPreviewHeight] = useState<number>(0)

  const updatePreviewHeight = (): void => {
    if (previewRef.current) {
      const {height} = previewRef.current.getBoundingClientRect()

      setPreviewHeight(height)
    }
  }

  useEffect(() => {
    updatePreviewHeight()
  }, [])

  const handleSwitchMode = (newMode: MarkdownMode): void => {
    if (newMode === 'edit') {
      updatePreviewHeight()
    }

    switchMode(newMode)
  }

  const handleTextareaChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    onChangeContents(e.target.value)
  }

  const contentsWithDefault = !!contents
    ? contents
    : 'Click `Edit` to add text here'

  let body = (
    <div className="notebook-panel--markdown markdown-format" ref={previewRef}>
      <MarkdownRenderer text={contentsWithDefault} />
    </div>
  )

  if (mode === 'edit') {
    const textAreaStyle = {height: `${previewHeight}px`}

    body = (
      <textarea
        className="notebook-panel--markdown-edit"
        value={contents}
        onChange={handleTextareaChange}
        style={textAreaStyle}
        autoFocus={true}
        autoComplete="off"
      />
    )
  }

  const controls = (
    <SelectGroup>
      <SelectGroup.Option
        onClick={handleSwitchMode}
        id="edit"
        value="edit"
        active={mode === 'edit'}
      >
        Edit
      </SelectGroup.Option>
      <SelectGroup.Option
        onClick={handleSwitchMode}
        id="preview"
        value="preview"
        active={mode === 'preview'}
      >
        Preview
      </SelectGroup.Option>
    </SelectGroup>
  )

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
