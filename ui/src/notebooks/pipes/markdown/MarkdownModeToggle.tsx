// Libraries
import React, {FC} from 'react'

// Components
import {SelectGroup} from '@influxdata/clockface'

// Types
import {MarkdownMode} from './'

interface Props {
  mode: MarkdownMode
  onToggleMode: (mode: MarkdownMode) => void
}

const MarkdownModeToggle: FC<Props> = ({mode, onToggleMode}) => {
  return (
    <SelectGroup>
      <SelectGroup.Option
        active={mode === 'edit'}
        onClick={onToggleMode}
        value="edit"
        id="edit"
      >
        Edit
      </SelectGroup.Option>
      <SelectGroup.Option
        active={mode === 'preview'}
        onClick={onToggleMode}
        value="preview"
        id="preview"
      >
        Preview
      </SelectGroup.Option>
    </SelectGroup>
  )
}

export default MarkdownModeToggle
