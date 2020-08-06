// Libraries
import React, {FC} from 'react'

// Components
import WriteDataIndexView from 'src/writeData/components/pageTemplates/WriteDataIndexView'

// Constants
import WRITE_DATA_CLIENT_LIBRARIES_SECTION from 'src/writeData/constants/contentClientLibraries'

const ClientLibrariesIndex: FC = ({children}) => {
  return (
    <>
      <WriteDataIndexView content={WRITE_DATA_CLIENT_LIBRARIES_SECTION} />
      {children}
    </>
  )
}

export default ClientLibrariesIndex
