import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {AppState} from 'src/types'
import Editor from 'src/shared/components/TomlMonacoEditor'
import * as monacoEditor from 'monaco-editor/esm/vs/editor/editor.api'
import {setText, setActivePlugins} from 'src/dataLoaders/actions/telegrafEditor'
import {TelegrafEditorPluginType} from 'src/dataLoaders/reducers/telegrafEditor'

const PLUGIN_REGEX = /\[\[\s*(inputs|outputs|processors|aggregators)\.(.+)\s*\]\]/

interface DispatchProps {
  onSetText: typeof setText
  onSetActivePlugins: typeof setActivePlugins
}

interface StateProps {
  script: string
}

type Props = ReduxProps

interface InterumMatchFormat {
  name: string
  type: TelegrafEditorPluginType
  line: number
}

class TelegrafEditorMonaco extends PureComponent<Props> {
  _editor: monacoEditor.editor.IStandaloneCodeEditor = null

  render() {
    const {script} = this.props

    return (
      <div className="telegraf-editor--middle-column">
        <div className="telegraf-editor--column-heading">
          <span className="telegraf-editor--title">Configuration File</span>
        </div>
        <Editor
          className="telegraf-editor--monaco"
          script={script}
          onChangeScript={this.handleChange}
          willMount={this.connect}
        />
      </div>
    )
  }

  private extractPluginList() {
    if (!this._editor) {
      return
    }

    const matches: Array<monacoEditor.editor.FindMatch> = this._editor
      .getModel()
      .findMatches(PLUGIN_REGEX as any, false, true, false, null, true)

    const plugins = matches.map(
      (m: monacoEditor.editor.FindMatch): InterumMatchFormat => {
        return {
          type: m.matches[1].slice(0, -1) as TelegrafEditorPluginType,
          name: m.matches[2],
          line: m.range.startLineNumber,
        }
      }
    )

    this.props.onSetActivePlugins(plugins)
  }

  private connect = (editor: monacoEditor.editor.IStandaloneCodeEditor) => {
    this._editor = editor
    this.extractPluginList()
  }

  private handleChange = (evt: string) => {
    this.extractPluginList()
    this.props.onSetText(evt)
  }

  public jump(line: number) {
    this._editor.revealLineInCenter(line)
  }

  public nextLine(): number {
    const position = this._editor.getPosition()
    const matches = this._editor
      .getModel()
      .findNextMatch(PLUGIN_REGEX as any, position, true, false, null, true)
    let lineNumber

    if (
      position.lineNumber === 1 ||
      !matches ||
      position.lineNumber > matches.range.startLineNumber
    ) {
      //add it to the bottom
      lineNumber = this._editor.getModel().getLineCount()
    } else {
      lineNumber = matches.range.startLineNumber - 1
    }

    return lineNumber
  }

  public insert(text: string, line: number) {
    this._editor.setPosition({column: 1, lineNumber: line})
    this._editor.executeEdits('', [
      {
        range: {
          startLineNumber: line,
          startColumn: 1,
          endLineNumber: line,
          endColumn: 1,
        } as monacoEditor.Range,
        text: text,
        forceMoveMarkers: true,
      },
    ])
  }
}

export {TelegrafEditorMonaco}

const mstp = (state: AppState) => {
  const map = state.telegrafEditorPlugins.reduce((prev, curr) => {
    prev[curr.name] = curr
    return prev
  }, {})

  const script =
    state.telegrafEditor.text ||
    map['__default__'].include
      .map((i: string) => {
        if (!map.hasOwnProperty(i)) {
          return ''
        }
        return map[i].config
      })
      .join('\n')

  return {
    script,
  }
}

const mdtp = {
  onSetActivePlugins: setActivePlugins,
  onSetText: setText,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp, null, {
  withRef: true,
})(TelegrafEditorMonaco)
