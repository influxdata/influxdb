// Libraries
import React, {PureComponent, SyntheticEvent, ChangeEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import classnames from 'classnames'

// Components
import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import {
  TelegrafEditorPluginState,
  TelegrafEditorPlugin,
  TelegrafEditorActivePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'
import GetResources from 'src/resources/components/GetResources'
import {
  Input,
  IconFont,
  ComponentSize,
  SquareButton,
} from '@influxdata/clockface'

// Actions
import {setFilter, setList} from 'src/dataLoaders/actions/telegrafEditor'

// Types
import {AppState, ResourceType} from 'src/types'

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

const AllPluginList = connect<PluginStateProps, {}>(mstp_2, null)(PluginList)

interface StateProps {
  filter: string
  show: boolean
}

interface DispatchProps {
  onSetFilter: typeof setFilter
  onSetList: typeof setList
}

interface OwnProps {
  onJump: (which: TelegrafEditorActivePlugin) => void
  onAdd: (which: TelegrafEditorPlugin) => void
}

type TelegrafEditorSidebarProps = ReduxProps & OwnProps

class TelegrafEditorSideBar extends PureComponent<TelegrafEditorSidebarProps> {
  private renderPlugins() {
    const {show, onAdd} = this.props
    if (!show) {
      return false
    }

    return (
      <GetResources resources={[ResourceType.Plugins]}>
        <AllPluginList onClick={onAdd} />
      </GetResources>
    )
  }

  render() {
    const {filter, show, onSetList, onSetFilter} = this.props
    const columnClassName = classnames('telegraf-editor--left-column', {
      'telegraf-editor--column__collapsed': !show,
    })
    const icon = show ? IconFont.EyeOpen : IconFont.EyeClosed
    const header = 'Browse & Add Plugins'

    return (
      <div className={columnClassName}>
        <div className="telegraf-editor--column-heading">
          <span className="telegraf-editor--title">{header}</span>
          <SquareButton
            icon={icon}
            size={ComponentSize.ExtraSmall}
            onClick={() => onSetList(!show)}
          />
        </div>
        {show && (
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
        )}
        {this.renderPlugins()}
        {!show && (
          <div className="telegraf-editor--title__collapsed">{header}</div>
        )}
      </div>
    )
  }
}

const mstp_3 = (state: AppState) => {
  const filter = state.telegrafEditor.filter
  const show = state.telegrafEditor.showList

  return {
    filter,
    show,
  }
}

const mdtp_3 = {
  onSetList: setList,
  onSetFilter: setFilter,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp_3,
  mdtp_3
)(TelegrafEditorSideBar)
