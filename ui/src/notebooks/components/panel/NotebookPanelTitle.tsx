// Libraries
import React, {FC, ChangeEvent, useContext} from 'react'
import {NotebookContext, PipeMeta} from 'src/notebooks/context/notebook'

interface Props {
  index: number
}

const NotebookPanelTitle: FC<Props> = ({index}) => {
  const {meta, updateMeta} = useContext(NotebookContext)
  const title = meta[index].title
  const onTitleChange = (value: string) => {
    updateMeta(index, {
      title: value,
    } as PipeMeta)
  }

  let sourceName
  let titleElement = <div className="notebook-panel--title">{title}</div>

  const onChange = (e: ChangeEvent<HTMLInputElement>): void => {
    onTitleChange(e.target.value)
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

  return (
    <>
      {sourceName}
      {titleElement}
    </>
  )
}

export default NotebookPanelTitle
