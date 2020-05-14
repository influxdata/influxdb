import React, {FC} from 'react'

import {Page} from '@influxdata/clockface'
import {NotebookProvider} from 'src/notebooks/context/notebook'

const NotebookPage: FC = () => {
  return (
    <NotebookProvider>
      <Page titleTag="Notebook">
        <Page.Header fullWidth={false} />
        <Page.Contents fullWidth={false} scrollable={true} />
      </Page>
    </NotebookProvider>
  )
}

export default NotebookPage
