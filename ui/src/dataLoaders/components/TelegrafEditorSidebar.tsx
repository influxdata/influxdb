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
  Grid,
  Columns,
  Tabs,
  ComponentSize,
  Orientation,
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
  mode: 'adding' | 'indexing'
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
      mode,
      onAdd,
      onJump,
      onSetMode,
      onSetBucket,
      onSetFilter,
    } = this.props
    return (
      <Grid.Column widthXS={Columns.Three} style={{height: '100%'}}>
        <FormElement label="Bucket">
          <BucketDropdown
            buckets={buckets}
            selectedBucketID={bucket.id}
            onSelectBucket={onSetBucket}
          />
        </FormElement>
        <Input
          className="wizard-step--filter"
          size={ComponentSize.Small}
          icon={IconFont.Search}
          value={filter}
          onBlur={(evt: SyntheticEvent<any>) => {
            onSetFilter((evt.target as any).value)
          }}
          onChange={(evt: ChangeEvent<any>) => {
            onSetFilter(evt.target.value)
          }}
          placeholder="Filter Plugins..."
        />
        <Tabs.Container
          orientation={Orientation.Horizontal}
          style={{height: 'calc(100% - 114px)', marginTop: '18px'}}
        >
          <Tabs>
            <Tabs.Tab
              id="lookup"
              text="Plugin Lookup"
              active={mode === 'indexing'}
              onClick={() => {
                onSetMode('indexing')
              }}
            />
            <Tabs.Tab
              id="add"
              text="Add Plugins"
              active={mode === 'adding'}
              onClick={() => {
                onSetMode('adding')
              }}
            />
          </Tabs>
          <Tabs.TabContents padding={ComponentSize.Small}>
            {mode === 'indexing' && <ActivePluginList onClick={onJump} />}
            {mode === 'adding' && <AllPluginList onClick={onAdd} />}
          </Tabs.TabContents>
        </Tabs.Container>
      </Grid.Column>
    )
  }
}

const mstp_3 = (state: AppState): StateProps => {
  const filter = state.telegrafEditor.filter
  const mode = state.telegrafEditor.mode
  const buckets = state.buckets.list || []
  const bucket =
    state.telegrafEditor.bucket || buckets.length
      ? buckets[0]
      : ({id: null} as Bucket)

  return {
    buckets,
    bucket,
    mode,
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
