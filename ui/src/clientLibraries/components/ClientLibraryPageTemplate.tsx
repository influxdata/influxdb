// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Page} from '@influxdata/clockface'

interface Props {
  title: string
  children: React.ReactNode
}

const ClientLibraryPage: FunctionComponent<Props> = ({title, children}) => {
  return (
    <Page>
      <Page.Header fullWidth={false}>
        <Page.Title title={title} />
      </Page.Header>
      <Page.Contents
        fullWidth={false}
        className="client-library-overlay"
        scrollable={true}
      >
        {children}
      </Page.Contents>
    </Page>
  )
}

export default ClientLibraryPage
