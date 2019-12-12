import React, {PureComponent, SyntheticEvent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import {AppState} from 'src/types'
import {
  TelegrafEditorPluginState,
  TelegrafEditorPlugin,
  TelegrafEditorActivePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'
import {
  setFilter,
  setMode,
} from 'src/dataLoaders/actions/telegrafEditor'
import {
  Input,
  IconFont,
  ComponentSize,
  SquareButton,
} from '@influxdata/clockface'

interface PluginStateProps {
  plugins: TelegrafEditorPluginState
  filter: string
}

const mstp_2 = (state: AppState): PluginStateProps => {
  const plugins = state.telegrafEditorPlugins || []
  const filter = state.telegrafEditor.filter

  return {
    plugins,
    filter,
  }
}

const AllPluginList = connect<PluginStateProps, {}>(
  mstp_2,
  null
)(PluginList)

interface StateProps {
  filter: string
}

interface DispatchProps {
  onSetFilter: typeof setFilter
  onSetMode: typeof setMode
}

interface OwnProps {
  onJump: (which: TelegrafEditorActivePlugin) => void
  onAdd: (which: TelegrafEditorPlugin) => void
}

type TelegrafEditorSidebarProps = StateProps & DispatchProps & OwnProps

class TelegrafEditorSideBar extends PureComponent<TelegrafEditorSidebarProps> {
  render() {
    const {
      filter,
      onAdd,
      onSetFilter,
    } = this.props
    const collapsed = true
    const columnClassName = classnames('telegraf-editor--left-column', { 'telegraf-editor--column__collapsed': collapsed })
    const icon = collapsed ? IconFont.EyeClosed : IconFont.EyeOpen

    return (
      <div className={columnClassName}>
        <div className="telegraf-editor--column-heading">
          <span className="telegraf-editor--title">Browse & Add Plugins</span>
          <SquareButton icon={icon} size={ComponentSize.ExtraSmall} />
        </div>
        <Input
          className="telegraf-editor--filter"
          size={ComponentSize.Small}
          icon={IconFont.Search}
          value={filter}
          onBlur={(evt: SyntheticEvent<any>) => {
            onSetFilter((evt.target as any).value)
          }}
          style={{}}
          onChange={(evt: ChangeEvent<any>) => {
            onSetFilter(evt.target.value)
          }}
          placeholder="Filter Plugins..."
        />
        <AllPluginList onClick={onAdd} />
      </div>
    )
  }
}

const mstp_3 = (state: AppState): StateProps => {
  const filter = state.telegrafEditor.filter

  return {
    filter,
  }
}

const mdtp_3: DispatchProps = {
  onSetMode: setMode,
  onSetFilter: setFilter,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp_3,
  mdtp_3
)(TelegrafEditorSideBar)
