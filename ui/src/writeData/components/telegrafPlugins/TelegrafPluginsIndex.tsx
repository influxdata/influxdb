// Libraries
import React, {FC} from 'react'

// Components
import {Page, SquareGrid, ComponentSize} from '@influxdata/clockface'
import WriteDataItem from 'src/writeData/components/WriteDataItem'

// Constants
import {WRITE_DATA_TELEGRAF_PLUGINS} from 'src/writeData/constants'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

const TelegrafPluginsIndex: FC = ({children}) => {
  return (
    <>
      <Page titleTag={pageTitleSuffixer(['Telegraf Plugins', 'Load Data'])}>
        <Page.Header fullWidth={false}>
          <Page.Title title="Telegraf Plugins" />
        </Page.Header>
        <Page.Contents fullWidth={false}>
          <SquareGrid cardSize="200px" gutter={ComponentSize.Small}>
            {WRITE_DATA_TELEGRAF_PLUGINS.map(plugin => (
              <WriteDataItem
                key={plugin.id}
                id={plugin.id}
                name={plugin.name}
                url={plugin.url}
                image={plugin.image}
              />
            ))}
          </SquareGrid>
        </Page.Contents>
      </Page>
      {children}
    </>
  )
}

export default TelegrafPluginsIndex
