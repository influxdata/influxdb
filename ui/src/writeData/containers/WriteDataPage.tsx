// Libraries
import React, {FC, useState, createContext} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import WriteDataSearchBar from 'src/writeData/components/WriteDataSearchBar'
import WriteDataSections from 'src/writeData/components/WriteDataSections'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

interface WriteDataSearchContextType {
  searchTerm: string
  setSearchTerm: (searchTerm: string) => void
}

export const WriteDataSearchContext = createContext<WriteDataSearchContextType>(
  {
    searchTerm: '',
    setSearchTerm: () => {},
  }
)

const WriteDataPage: FC = () => {
  const [searchTerm, setSearchTerm] = useState<string>('')

  const FULL_WIDTH = false

  return (
    <WriteDataSearchContext.Provider value={{searchTerm, setSearchTerm}}>
      <Page titleTag={pageTitleSuffixer(['Write Data', 'Load Data'])}>
        <Page.Header fullWidth={FULL_WIDTH}>
          <Page.Title title="Write Data to InfluxDB Cloud" />
        </Page.Header>
        <Page.ControlBar fullWidth={FULL_WIDTH}>
          <WriteDataSearchBar />
        </Page.ControlBar>
        <Page.Contents fullWidth={FULL_WIDTH} scrollable={true}>
          <WriteDataSections />
        </Page.Contents>
      </Page>
    </WriteDataSearchContext.Provider>
  )
}

export default WriteDataPage
