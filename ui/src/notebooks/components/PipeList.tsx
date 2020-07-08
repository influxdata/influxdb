// Libraries
import React, {FC, useContext} from 'react'

// Contexts
import {NotebookContext} from 'src/notebooks/context/notebook.current'
import {ScrollContext} from 'src/notebooks/context/scroll'

// Components
import NotebookPipe from 'src/notebooks/components/NotebookPipe'
import EmptyPipeList from 'src/notebooks/components/EmptyPipeList'
import {DapperScrollbars} from '@influxdata/clockface'

const PipeList: FC = () => {
  const {scrollPosition} = useContext(ScrollContext)
  const {notebook} = useContext(NotebookContext)
  const {data} = notebook

  if (!data || !data.allIDs.length) {
    return <EmptyPipeList />
  }

  const _pipes = data.allIDs.map(id => {
    return <NotebookPipe key={`pipe-${id}`} id={id} />
  })

  return (
    <DapperScrollbars
      className="notebook-main"
      autoHide={true}
      noScrollX={true}
      scrollTop={scrollPosition}
    >
      {_pipes}
    </DapperScrollbars>
  )
}

export default PipeList
