// Libraries
import React, {PureComponent, createRef} from 'react'
import {connect} from 'react-redux'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {AppState} from 'src/types'
import {Grid, Columns} from '@influxdata/clockface'
import TelegrafEditorSidebar from 'src/dataLoaders/components/TelegrafEditorSidebar'
import TelegrafEditorMonaco from 'src/dataLoaders/components/TelegrafEditorMonaco'

import {
  TelegrafEditorActivePlugin,
  TelegrafEditorBasicPlugin,
  TelegrafEditorBundlePlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

import './TelegrafEditor.scss'

interface StateProps {
  [map: string]: any
}

type ListPlugin = TelegrafEditorBasicPlugin | TelegrafEditorBundlePlugin
interface OwnProps {
  onJump: (which: TelegrafEditorActivePlugin) => void
  onAdd: (which: ListPlugin) => void
}

type Props = StateProps & OwnProps

@ErrorHandling
class TelegrafEditor extends PureComponent<Props> {
  _editor = createRef()

  render() {
    return (
      <Grid style={{height: '100%'}}>
        <Grid.Row style={{textAlign: 'center'}}>
          <h3 className="wizard-step--title">What do you want to monitor?</h3>
          <h5 className="wizard-step--sub-title">
            Telegraf is a plugin-based data collection agent which writes
            metrics to a bucket in InfluxDB
            <br />
            Use the editor below to configure as many of the 200+{' '}
            <a
              href="https://v2.docs.influxdata.com/v2.0/reference/telegraf-plugins/#input-plugins"
              target="_blank"
            >
              plugins
            </a>{' '}
            as you require
          </h5>
        </Grid.Row>
        <Grid.Row style={{height: 'calc(100% - 128px)'}}>
          <TelegrafEditorSidebar
            onJump={this.handleJump}
            onAdd={this.handleAdd}
          />
          <Grid.Column widthXS={Columns.Nine} style={{height: '100%'}}>
            <TelegrafEditorMonaco ref={this._editor} />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleJump = (which) => {
    this._editor.getWrappedInstance().jump(which.line)
  }

  private handleAdd = (which) => {
    const editor = this._editor.getWrappedInstance()
    const line = editor.nextLine()

    if (which.type === 'bundle') {
      (which.include || [])
        .map(item => (this.props.map[item] || {}).code)
        .filter(i => !!i)
        .reverse()
        .forEach(item => {
          editor.insert(item, line)
        })
    } else {
      editor.insert(which.code || '', line)
    }

    editor.jump(line)
  }
}

const mstp = (state: AppState): StateProps => {
  const map = state.telegrafEditorPlugins.reduce((prev, curr) => {
    prev[curr.name] = curr
    return prev
  }, {})

  return {
    map,
  }
}

export default connect<StateProps, {}>(
  mstp,
  null
)(TelegrafEditor)
