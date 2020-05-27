// Libraries
import React, {FC, useContext, useCallback} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook'

// Components
import NotebookPipe from 'src/notebooks/components/NotebookPipe'
import EmptyPipeList from 'src/notebooks/components/EmptyPipeList'
import {Page} from '@influxdata/clockface'

const PipeList: FC = () => {
  const {id, pipes, updatePipe} = useContext(NotebookContext)
  const update = useCallback(updatePipe, [id])

  const scrollable = !!pipes.length

  let _pipes: JSX.Element | JSX.Element[] = <EmptyPipeList />

  if (pipes.length) {
    _pipes = pipes.map((_, index) => {
      return (
        <NotebookPipe
          key={`pipe-${id}-${index}`}
          index={index}
          data={pipes[index]}
          onUpdate={update}
        />
      )
    })
  }

  return (
    <Page.Contents fullWidth={true} scrollable={scrollable}>
      {_pipes}
    </Page.Contents>
  )
}

export default PipeList
