// Libraries
import React, {FC, ReactNode} from 'react'
import {useParams} from 'react-router-dom'
import ReactMarkdown, {Renderer} from 'react-markdown'

// Components
import {Page} from '@influxdata/clockface'
import WriteDataCodeSnippet from 'src/writeData/components/WriteDataCodeSnippet'
import WriteDataHelper from 'src/writeData/components/WriteDataHelper'
import WriteDataDetailsContextProvider from 'src/writeData/components/WriteDataDetailsContext'
import GetResources from 'src/resources/components/GetResources'

// Types
import {WriteDataSection} from 'src/writeData/constants'
import {ResourceType} from 'src/types'

// Graphics
import placeholderLogo from 'src/writeData/graphics/placeholderLogo.svg'

// Styles
import 'src/writeData/components/WriteDataDetailsView.scss'

interface Props {
  section: WriteDataSection
  children?: ReactNode
}

const codeRenderer: Renderer<HTMLPreElement> = (props: any): any => {
  return <WriteDataCodeSnippet code={props.value} language={props.language} />
}

const WriteDataDetailsView: FC<Props> = ({section, children}) => {
  const {contentID} = useParams()
  const {name, markdown, image} = section.items.find(
    item => item.id === contentID
  )

  let thumbnail = <img src={image || placeholderLogo} />
  let pageContent = <></>

  if (image) {
    thumbnail = <img src={image} />
  }

  if (markdown) {
    pageContent = (
      <ReactMarkdown source={markdown} renderers={{code: codeRenderer}} />
    )
  }

  return (
    <GetResources
      resources={[ResourceType.Authorizations, ResourceType.Buckets]}
    >
      <WriteDataDetailsContextProvider>
        <Page>
          <Page.Header fullWidth={false}>
            <Page.Title title={name} />
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="write-data--details">
              <div className="write-data--details-thumbnail">{thumbnail}</div>
              <div className="write-data--details-content markdown-format">
                {children}
                <WriteDataHelper />
                {pageContent}
              </div>
            </div>
          </Page.Contents>
        </Page>
      </WriteDataDetailsContextProvider>
    </GetResources>
  )
}

export default WriteDataDetailsView
