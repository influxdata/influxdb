// Libraries
import React, {FC, ChangeEvent} from 'react'

// Components
import {Icon, IconFont} from '@influxdata/clockface'

interface Props {
  title: string
  onTitleChange?: (title: string) => void
  previousPanelTitle?: string
}

const NotebookPanelTitle: FC<Props> = ({
  title,
  onTitleChange,
  previousPanelTitle,
}) => {
  let sourceName
  let titleElement = <div className="notebook-panel--title">{title}</div>

  if (previousPanelTitle) {
    sourceName = (
      <div className="notebook-panel--data-source">
        {previousPanelTitle}
        <Icon
          glyph={IconFont.CaretRight}
          className="notebook-panel--data-caret"
        />
      </div>
    )
  }

  if (onTitleChange) {
    const onChange = (e: ChangeEvent<HTMLInputElement>): void => {
      const trimmedValue = e.target.value.replace(' ', '_')
      onTitleChange(trimmedValue)
    }

    titleElement = (
      <input
        type="text"
        value={title}
        onChange={onChange}
        placeholder="Enter an ID"
        className="notebook-panel--editable-title"
        autoComplete="off"
        autoCorrect="off"
        spellCheck={false}
        maxLength={30}
      />
    )
  }

  return (
    <>
      {sourceName}
      {titleElement}
    </>
  )
}

export default NotebookPanelTitle
