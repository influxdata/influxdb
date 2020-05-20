// Libraries
import React, {FC, ChangeEvent} from 'react'

interface Props {
  text: string
  onChange: (text: string) => void
}

const MarkdownPanelEditor: FC<Props> = ({text, onChange}) => {
  const handleTextAreaChange = (e: ChangeEvent<HTMLTextAreaElement>): void => {
    onChange(e.target.value)
  }

  return (
    <textarea className="notebook-panel--markdown-editor" value={text} onChange={handleTextAreaChange} />
  )
}

export default MarkdownPanelEditor
