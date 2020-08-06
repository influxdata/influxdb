// Libraries
import React, {FC, ReactNode} from 'react'
import ReactMarkdown, {Renderer} from 'react-markdown'

// Components
import {Page} from '@influxdata/clockface'
import CodeSnippet from 'src/shared/components/CodeSnippet'

interface Props {
  title: string
  markdown: string
  children?: ReactNode
}

const codeRenderer: Renderer<HTMLPreElement> = (props: any): any => {
  return <CodeSnippet copyText={props.value} label={props.language} />
}

const TelegrafPluginPage: FC<Props> = ({title, children, markdown}) => {
  return (
    <Page>
      <Page.Header fullWidth={false}>
        <Page.Title title={title} />
      </Page.Header>
      <Page.Contents fullWidth={false} scrollable={true}>
        <ReactMarkdown source={markdown} renderers={{code: codeRenderer}} />
        {children}
      </Page.Contents>
    </Page>
  )
}

export default TelegrafPluginPage
