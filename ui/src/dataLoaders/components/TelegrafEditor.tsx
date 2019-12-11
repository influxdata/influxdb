// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Grid, Columns} from '@influxdata/clockface'
import TelegrafEditorSidebar from 'src/dataLoaders/components/TelegrafEditorSidebar'
import TelegrafEditorMonaco from 'src/dataLoaders/components/TelegrafEditorMonaco'

// Styles
import './TelegrafEditor.scss'

// Utils
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState} from 'src/types'
import {
  TelegrafEditorActivePlugin,
  TelegrafEditorPlugin,
  TelegrafEditorBasicPlugin,
} from 'src/dataLoaders/reducers/telegrafEditor'

type AllPlugin = TelegrafEditorPlugin | TelegrafEditorActivePlugin

interface StateProps {
  pluginHashMap: {[k: string]: AllPlugin}
}

type Props = StateProps

@ErrorHandling
class TelegrafEditor extends PureComponent<Props> {
  _editor: any = null

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
            <TelegrafEditorMonaco ref={this.connect} />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private connect = (elem: any) => {
    this._editor = elem
  }

  private handleJump = (which: TelegrafEditorActivePlugin) => {
    this._editor.getWrappedInstance().jump(which.line)
  }

  private handleAdd = (which: TelegrafEditorPlugin) => {
    const editor = this._editor.getWrappedInstance()
    const line = editor.nextLine()

    if (which.type === 'bundle') {
      which.include
        .filter(
          item =>
            this.props.pluginHashMap[item] &&
            this.props.pluginHashMap[item].type !== 'bundle'
        )
        .map(
          item =>
            (
              (this.props.pluginHashMap[item] as TelegrafEditorBasicPlugin) ||
              {}
            ).code
        )
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
  const pluginHashMap = state.telegrafEditorPlugins
    .filter(
      (a: TelegrafEditorPlugin) => a.type !== 'bundle' || !!a.include.length
    )
    .reduce((prev, curr) => {
      prev[curr.name] = curr
      return prev
    }, {})

  return {
    pluginHashMap,
  }
}

export default connect<StateProps, {}>(
  mstp,
  null
)(TelegrafEditor)
