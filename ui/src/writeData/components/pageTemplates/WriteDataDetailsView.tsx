// Libraries
import React, {FC} from 'react'
import ReactMarkdown, {Renderer} from 'react-markdown'

// Components
import {Page} from '@influxdata/clockface'
import CodeSnippet from 'src/shared/components/CodeSnippet'

// Types
import {WriteDataSection} from 'src/writeData/constants'

// Graphics
import placeholderLogo from 'src/writeData/graphics/placeholderLogo.svg'

interface Props {
  itemID: string
  section: WriteDataSection
}

const codeRenderer: Renderer<HTMLPreElement> = (props: any): any => {
  return <CodeSnippet copyText={props.value} label={props.language} />
}

const WriteDataDetailsView: FC<Props> = ({itemID, section, children}) => {
  const {name, markdown, image} = section.items.find(item => item.id === itemID)

  let thumbnail = <img src={placeholderLogo} />

  if (image) {
    thumbnail = <img src={image} />
  }

  return (
    <Page>
      <Page.Header fullWidth={false}>
        <Page.Title title={name} />
      </Page.Header>
      <Page.Contents fullWidth={false} scrollable={true}>
        {thumbnail}
        {children}
        <ReactMarkdown source={markdown} renderers={{code: codeRenderer}} />
      </Page.Contents>
    </Page>
  )
}

export default WriteDataDetailsView
