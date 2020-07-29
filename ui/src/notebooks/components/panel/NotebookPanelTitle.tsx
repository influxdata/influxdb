// Libraries
import React, {FC, ChangeEvent, useContext} from 'react'
import {NotebookContext} from 'src/notebooks/context/notebook.current'

interface Props {
  id: string
}

const NotebookPanelTitle: FC<Props> = ({id}) => {
  const {notebook} = useContext(NotebookContext)
  const title = notebook.meta.get(id).title
  const onTitleChange = (value: string) => {
    notebook.meta.update(id, {
      title: value,
    })
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
