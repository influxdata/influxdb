import React, {PureComponent, SyntheticEvent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import PluginList from 'src/dataLoaders/components/TelegrafEditorPluginList'
import BucketDropdown from 'src/dataLoaders/components/BucketsDropdown'
import {AppState, Bucket} from 'src/types'
import {
  TelegrafEditorPluginState,
  TelegrafEditorActivePluginState,
  TelegrafEditorPlugin,
  TelegrafEditorActivePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'
import {
  setFilter,
  setBucket,
  setMode,
} from 'src/dataLoaders/actions/telegrafEditor'
import {
  Input,
  IconFont,
  FormElement,
  ComponentSize,
} from '@influxdata/clockface'

interface PluginStateProps {
  plugins: TelegrafEditorPluginState
  filter: string
}

interface ActivePluginStateProps {
  plugins: TelegrafEditorActivePluginState
  filter: string
}

const mstp_1 = (state: AppState): ActivePluginStateProps => {
  const plugins = state.telegrafEditorActivePlugins || []
  const filter = state.telegrafEditor.filter

  return {
    plugins,
    filter,
  }
}

const ActivePluginList = connect<ActivePluginStateProps, {}>(
  mstp_1,
  null
)(PluginList)

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
  buckets: Bucket[]
  bucket: Bucket
  filter: string
}

interface DispatchProps {
  onSetFilter: typeof setFilter
  onSetBucket: typeof setBucket
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
      bucket,
      buckets,
      filter,
      onAdd,
      onSetBucket,
      onSetFilter,
    } = this.props
    return (
      <div className="telegraf-editor--left-column">
        <div className="telegraf-editor--title">Browse & Add Plugins</div>
        {/* <FormElement label="Bucket">
          <BucketDropdown
            buckets={buckets}
            selectedBucketID={bucket.id}
            onSelectBucket={onSetBucket}
          />
        </FormElement> */}
        {/* <div className="telegraf-editor--column-section">
          <p>Want access to all 200+ plugins?<br/>
          <a
            href="https://v2.docs.influxdata.com/v2.0/reference/telegraf-plugins/#input-plugins"
            target="_blank"
          >
            See the full list
            </a>{' '}and add them manually
          </p>
        </div> */}
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
  const buckets = state.buckets.list || []
  const bucket =
    state.telegrafEditor.bucket || buckets.length
      ? buckets[0]
      : ({id: null} as Bucket)

  return {
    buckets,
    bucket,
    filter,
  }
}

const mdtp_3: DispatchProps = {
  onSetMode: setMode,
  onSetBucket: setBucket,
  onSetFilter: setFilter,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp_3,
  mdtp_3
)(TelegrafEditorSideBar)
