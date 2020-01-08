// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {
  Form,
  Input,
  InputType,
  Grid,
  Columns,
  ComponentSize,
} from '@influxdata/clockface'
import TelegrafEditorSidebar from 'src/dataLoaders/components/TelegrafEditorSidebar'
import TelegrafEditorPluginLookup from 'src/dataLoaders/components/TelegrafEditorPluginLookup'
import TelegrafEditorMonaco from 'src/dataLoaders/components/TelegrafEditorMonaco'
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'

// Actions
import {setName, setDescription} from 'src/dataLoaders/actions/telegrafEditor'

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
  showSetup: boolean
  name: string
  description: string
}

interface DispatchProps {
  onSetName: typeof setName
  onSetDescription: typeof setDescription
}

type Props = DispatchProps & StateProps

@ErrorHandling
class TelegrafEditor extends PureComponent<Props> {
  _editor: any = null

  render() {
    const {name, description, showSetup} = this.props

    if (showSetup) {
      return (
        <TelegrafInstructions
          token="INFLUX_TOKEN"
          configID="INFLUX_CONFIG_ID"
        />
      )
    }

    return (
      <div className="telegraf-editor">
        <div className="telegraf-editor--heading">
          <Grid>
            <Grid.Row>
              <Grid.Column widthMD={Columns.Four}>
                <Form.Element label="Configuration Name">
                  <Input
                    type={InputType.Text}
                    value={name}
                    name="name"
                    onChange={this.handleNameInput}
                    titleText="Configuration Name"
                    size={ComponentSize.Medium}
                    autoFocus={true}
                  />
                </Form.Element>
              </Grid.Column>
              <Grid.Column widthMD={Columns.Eight}>
                <Form.Element label="Description">
                  <Input
                    type={InputType.Text}
                    value={description}
                    name="description"
                    onChange={this.handleDescriptionInput}
                    titleText="Description"
                    size={ComponentSize.Medium}
                  />
                </Form.Element>
              </Grid.Column>
            </Grid.Row>
          </Grid>
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

  private handleNameInput = (e: ChangeEvent<HTMLInputElement>) => {
    this.props.onSetName(e.target.value)
  }

  private handleDescriptionInput = (e: ChangeEvent<HTMLInputElement>) => {
    this.props.onSetDescription(e.target.value)
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
  const {name, description, showSetup} = state.telegrafEditor

  return {
    pluginHashMap,
    name,
    description,
    showSetup,
  }
}

const mdtp: DispatchProps = {
  onSetName: setName,
  onSetDescription: setDescription,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TelegrafEditor)
