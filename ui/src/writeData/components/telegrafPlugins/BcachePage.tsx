// Libraries
import React, {FC} from 'react'

// Components
import WriteDataDetailsView from 'src/writeData/components/pageTemplates/WriteDataDetailsView'

// Constants
import WRITE_DATA_TELEGRAF_PLUGINS_SECTION from 'src/writeData/constants/contentTelegrafPlugins'

const BcachePage: FC = () => {
  return (
    <WriteDataDetailsView
      itemID="bcache"
      section={WRITE_DATA_TELEGRAF_PLUGINS_SECTION}
    />
  )
}

export default BcachePage
