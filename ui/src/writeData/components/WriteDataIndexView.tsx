// Libraries
import React, {FC} from 'react'

// Components
import {Page, SquareGrid, ComponentSize} from '@influxdata/clockface'
import WriteDataItem from 'src/writeData/components/WriteDataItem'

// Constants
import {WriteDataSection} from 'src/writeData/constants'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

interface Props {
  content: WriteDataSection
}

const WriteDataIndexView: FC<Props> = ({children, content}) => {
  const {items, name} = content

  return (
    <>
      <Page titleTag={pageTitleSuffixer([name, 'Load Data'])}>
        <Page.Header fullWidth={false}>
          <Page.Title title={name} />
        </Page.Header>
        <Page.Contents fullWidth={false} scrollable={true}>
          <SquareGrid cardSize="200px" gutter={ComponentSize.Small}>
            {items.map(item => (
              <WriteDataItem
                key={item.id}
                id={item.id}
                name={item.name}
                url={item.url}
                image={item.image}
              />
            ))}
          </SquareGrid>
        </Page.Contents>
      </Page>
      {children}
    </>
  )
}

export default WriteDataIndexView
