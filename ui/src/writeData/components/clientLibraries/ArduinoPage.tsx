// Libraries
import React, {FunctionComponent} from 'react'

// Components
import TelegrafPluginPage from 'src/writeData/components/telegrafPlugins/TelegrafPluginPage'

// Markdown
import markdown from 'src/writeData/components/clientLibraries/Arduino.md'

const ClientArduinoPage: FunctionComponent = () => {
  return (
    <TelegrafPluginPage title="Arduino Client Library" markdown={markdown} />
  )
}

export default ClientArduinoPage
