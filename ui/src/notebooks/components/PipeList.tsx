// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'

// Components
import NotebookPipe from 'src/notebooks/components/NotebookPipe'
import EmptyPipeList from 'src/notebooks/components/EmptyPipeList'
import {DapperScrollbars} from '@influxdata/clockface'

const PipeList: FC = () => {
  const {id, pipes, updatePipe, listScrollPosition} = useContext(
    NotebookContext
  )
  const update = useCallback(updatePipe, [id])

  if (!pipes.length) {
    return <EmptyPipeList />
  }

  const _pipes = pipes.map((_, index) => {
    return (
      <NotebookPipe
        key={`pipe-${id}-${index}`}
        index={index}
        data={pipes[index]}
        onUpdate={update}
      />
    )
  })

  return (
    <DapperScrollbars
      className="notebook-main"
      autoHide={true}
      noScrollX={true}
      scrollTop={listScrollPosition}
    >
      {_pipes}
    </DapperScrollbars>
  )
}

export default PipeList
