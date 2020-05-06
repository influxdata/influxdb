import React, {FC, useContext} from 'react'

import {Page} from '@influxdata/clockface'
import NotebookPipe, {AddMorePipes} from 'src/notebooks'
import {NotebookProvider, NotebookContext} from 'src/notebooks/notebook.context'

const NotebookHeader: FC = () => {
  //const { id } = useContext(NotebookContext)
  //const { context, updateContext } = useContext(TimeContext)

  return (
    <>
      <h1>NOTEBOOKS</h1>
    </>
  )
}

const NotebookList: FC = () => {
  const {id, pipes} = useContext(NotebookContext)
  const _pipes = pipes.map((_, idx) => (
    <NotebookPipe idx={idx} key={`pipe-${id}-${idx}`} />
  ))

  return <>{_pipes}</>
}

const NotebookPage: FC = () => {
  return (
    <Page titleTag={'Notebook'}>
      <Page.Header fullWidth={false}>
        <NotebookHeader />
      </Page.Header>
      <Page.Contents fullWidth={false} scrollable={true}>
        <NotebookProvider>
          <NotebookList />
          <AddMorePipes />
        </NotebookProvider>
      </Page.Contents>
    </Page>
  )
}

export default NotebookPage
