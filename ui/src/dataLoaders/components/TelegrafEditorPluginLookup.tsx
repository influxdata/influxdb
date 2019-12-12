import React, {FC} from 'react'
import {connect} from 'react-redux'
import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import {AppState} from 'src/types'
import {
  TelegrafEditorActivePluginState,
  TelegrafEditorActivePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

interface PluginStateProps {
  plugins: TelegrafEditorActivePluginState
}

interface OwnProps {
  onJump: (which: TelegrafEditorActivePlugin) => void
}

type Props = OwnProps & PluginStateProps

const TelegrafEditorSideBar: FC<Props> = ({plugins, onJump}) => {
  return (
    <div className="telegraf-editor--right-column">
<div className="telegraf-editor--title">{' '}</div>
      <PluginList plugins={plugins} filter="" onClick={onJump} />
    </div>
  )
}

const mstp = (state: AppState): PluginStateProps => {
  const plugins = state.telegrafEditorActivePlugins || []

  return {plugins}
}

export default connect<PluginStateProps, {}>(
  mstp,
  null
)(TelegrafEditorSideBar)
