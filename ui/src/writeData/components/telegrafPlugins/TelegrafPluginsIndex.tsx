// Libraries
import React, {FC} from 'react'

// Components
import WriteDataIndexView from 'src/writeData/components/pageTemplates/WriteDataIndexView'

// Constants
import WRITE_DATA_TELEGRAF_PLUGINS_SECTION from 'src/writeData/constants/contentTelegrafPlugins'

const TelegrafPluginsIndex: FC = ({children}) => {
  return (
    <>
      <WriteDataIndexView content={WRITE_DATA_TELEGRAF_PLUGINS_SECTION} />
      {children}
    </>
  )
}

export default TelegrafPluginsIndex
