// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Alert, ComponentColor} from '@influxdata/clockface'
import TelegrafEditorSidebar from 'src/dataLoaders/components/TelegrafEditorSidebar'
import TelegrafEditorPluginLookup from 'src/dataLoaders/components/TelegrafEditorPluginLookup'
import TelegrafEditorMonaco from 'src/dataLoaders/components/TelegrafEditorMonaco'

// Styles
import './TelegrafEditor.scss'

// Utils
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {AppState} from 'src/types'
import {
  TelegrafEditorActivePlugin,
  TelegrafEditorBasicPlugin,
  TelegrafEditorPlugin,
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
      <div className="telegraf-editor">
        <div className="telegraf-editor--heading">
          <Alert color={ComponentColor.Default}>
            This tool will help create a configuration file for Telegraf, but
            you will have to download and run Telegraf externally to get data
            into your bucket.
          </Alert>
        </div>
        <div className="telegraf-editor--body">
          <TelegrafEditorSidebar
            onJump={this.handleJump}
            onAdd={this.handleAdd}
          />
          <TelegrafEditorMonaco ref={this.connect} />
          <TelegrafEditorPluginLookup onJump={this.handleJump} />
        </div>
      </div>
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
              ({} as TelegrafEditorBasicPlugin)
            ).config
        )
        .filter(i => !!i)
        .reverse()
        .forEach(item => {
          editor.insert(item, line)
        })
    } else {
      editor.insert(which.config || '', line)
    }

    editor.jump(line)
  }
}

const mstp = (state: AppState) => {
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

export default connect<StateProps>(mstp)(TelegrafEditor)
