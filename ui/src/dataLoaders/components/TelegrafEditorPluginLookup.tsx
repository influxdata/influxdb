// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Components
import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import {AppState} from 'src/types'
import {SquareButton, IconFont, ComponentSize} from '@influxdata/clockface'

// Types
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
  // This is the new version of "mode"
  const collapsed = true
  const columnClassName = classnames('telegraf-editor--right-column', {'telegraf-editor--column__collapsed': collapsed})
  const icon = collapsed ? IconFont.EyeClosed : IconFont.EyeOpen

  return (
    <div className={columnClassName}>
      <div className="telegraf-editor--column-heading">
        <span className="telegraf-editor--title">Title?</span>
        <SquareButton icon={icon} size={ComponentSize.ExtraSmall} />
      </div>
      {!collapsed && <PluginList plugins={plugins} filter="" onClick={onJump} />}
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
