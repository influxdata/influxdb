// Libraries
import React, {FC, useState, createContext} from 'react'

// Components
import {Page} from '@influxdata/clockface'
import WriteDataSearchBar from 'src/writeData/components/WriteDataSearchBar'
import WriteDataSections from 'src/writeData/components/WriteDataSections'
import LoadDataHeader from 'src/settings/components/LoadDataHeader'
import LoadDataTabbedPage from 'src/settings/components/LoadDataTabbedPage'

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

  return (
    <WriteDataSearchContext.Provider value={{searchTerm, setSearchTerm}}>
      <Page titleTag={pageTitleSuffixer(['Sources', 'Load Data'])}>
        <LoadDataHeader />
        <LoadDataTabbedPage activeTab="sources">
          <WriteDataSearchBar />
          <WriteDataSections />
        </LoadDataTabbedPage>
      </Page>
    </WriteDataSearchContext.Provider>
  )
}

export default WriteDataPage
