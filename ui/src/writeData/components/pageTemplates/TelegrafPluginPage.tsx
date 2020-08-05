// Libraries
import React, {FC, ReactNode} from 'react'

// Components
import {Page} from '@influxdata/clockface'

interface Props {
  title: string
  children?: ReactNode
}

const TelegrafPluginPage: FC<Props> = ({title, children}) => {
  return (
    <Page>
      <Page.Header fullWidth={false}>
        <Page.Title title={title} />
      </Page.Header>
      <Page.Contents fullWidth={false} scrollable={true}>
        {children}
      </Page.Contents>
    </Page>
  )
}

export default TelegrafPluginPage
