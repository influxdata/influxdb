import React, {FC, useContext} from 'react'

import {Page} from '@influxdata/clockface'
import NotebookHeader from 'src/notebooks/components/NotebookHeader'
import NotebookPipe from 'src/notebooks/components/NotebookPipe'
import NotebookButtons from 'src/notebooks/components/NotebookButtons'
import {NotebookProvider, NotebookContext} from 'src/notebooks/notebook.context'

const NotebookList: FC = () => {
  const {id, pipes} = useContext(NotebookContext)
  const _pipes = pipes.map((_, idx) => (
    <NotebookPipe idx={idx} key={`pipe-${id}-${idx}`} />
  ))

  return <>{_pipes}</>
}

const NotebookPage: FC = () => {
  return (
        <NotebookProvider>
    <Page titleTag={'Notebook'}>
      <Page.Header fullWidth={false}>
        <NotebookHeader />
      </Page.Header>
      <Page.Contents fullWidth={false} scrollable={true}>
          <NotebookList />
          <NotebookButtons />
      </Page.Contents>
    </Page>
        </NotebookProvider>
  )
}

export default NotebookPage
