// Libraries
import React, {FC} from 'react'

// Components
import TelegrafPluginPage from 'src/writeData/components/telegrafPlugins/TelegrafPluginPage'

// Markdown
import markdown from 'src/writeData/components/telegrafPlugins/Bcache.md'

const BcachePage: FC = () => {
  return <TelegrafPluginPage title="Bcache" markdown={markdown} />
}

export default BcachePage
