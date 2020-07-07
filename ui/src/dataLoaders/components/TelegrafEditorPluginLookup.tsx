// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import classnames from 'classnames'

// Components
import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import {AppState} from 'src/types'
import {SquareButton, IconFont, ComponentSize} from '@influxdata/clockface'
import {setLookup} from 'src/dataLoaders/actions/telegrafEditor'

// Types
import {
  TelegrafEditorActivePluginState,
  TelegrafEditorActivePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

interface PluginStateProps {
  plugins: TelegrafEditorActivePluginState
  show: boolean
}

interface OwnProps {
  onJump: (which: TelegrafEditorActivePlugin) => void
}

interface PluginDispatchProps {
  onChangeLookup: typeof setLookup
}

type Props = OwnProps & PluginStateProps & PluginDispatchProps

const TelegrafEditorSideBar: FC<Props> = ({
  plugins,
  onJump,
  show,
  onChangeLookup,
}) => {
  const columnClassName = classnames('telegraf-editor--right-column', {
    'telegraf-editor--column__collapsed': !show,
  })
  const icon = show ? IconFont.EyeOpen : IconFont.EyeClosed
  const header = 'Plugins'

  return (
    <div className={columnClassName}>
      <div className="telegraf-editor--column-heading">
        <span className="telegraf-editor--title">{header}</span>
        <SquareButton
          icon={icon}
          size={ComponentSize.ExtraSmall}
          onClick={() => onChangeLookup(!show)}
        />
      </div>
      {show && <PluginList plugins={plugins} filter="" onClick={onJump} />}
      {!show && (
        <span className="telegraf-editor--title__collapsed">{header} </span>
      )}
    </div>
  )
}

const mstp = (state: AppState): PluginStateProps => {
  const plugins = state.telegrafEditorActivePlugins || []
  const show = state.telegrafEditor.showLookup

  return {plugins, show}
}

const mdtp: PluginDispatchProps = {
  onChangeLookup: setLookup,
}

export default connect<PluginStateProps, PluginDispatchProps>(
  mstp,
  mdtp
)(TelegrafEditorSideBar)
