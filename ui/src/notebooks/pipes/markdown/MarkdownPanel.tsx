// Libraries
import React, {FC} from 'react'

// Types
import {PipeProp} from 'src/notebooks'
import {MarkdownMode} from './'

// Components
import MarkdownModeToggle from './MarkdownModeToggle'

const MarkdownPanel: FC<PipeProp> = ({data, Context, onUpdate}) => {
  const handleToggleMode = (mode: MarkdownMode): void => {
    const updatedData = {...data, mode}

    onUpdate(updatedData)
  }

  const controls = (
    <MarkdownModeToggle mode={data.mode} onToggleMode={handleToggleMode} />
  )

  return (
    <Context controls={controls}>
      {data.mode}
      <h1>{data.text}</h1>
    </Context>
  )
}

export default MarkdownPanel
