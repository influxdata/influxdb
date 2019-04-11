// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Controlled as ReactCodeMirror} from 'react-codemirror2'
import {RemoteDataState} from '@influxdata/clockface'

// Actions
import {getTelegrafConfigToml} from 'src/telegrafs/actions'

// Types
import {AppState} from 'src/types'

interface DispatchProps {
  getTelegrafConfigToml: typeof getTelegrafConfigToml
}

interface StateProps {
  telegrafConfig: string
  status: RemoteDataState
}

type Props = StateProps & DispatchProps

@ErrorHandling
export class TelegrafConfig extends PureComponent<Props & WithRouterProps> {
  public componentDidMount() {
    const {
      params: {id},
      getTelegrafConfigToml,
    } = this.props
    getTelegrafConfigToml(id)
  }

  public render() {
    return <>{this.overlayBody}</>
  }

  private onBeforeChange = () => {}
  private onTouchStart = () => {}

  private get overlayBody(): JSX.Element {
    const options = {
      tabIndex: 1,
      mode: 'toml',
      readonly: true,
      lineNumbers: true,
      autoRefresh: true,
      theme: 'time-machine',
      completeSingle: false,
    }
    const {telegrafConfig} = this.props
    return (
      <ReactCodeMirror
        autoFocus={true}
        autoCursor={true}
        value={telegrafConfig}
        options={options}
        onBeforeChange={this.onBeforeChange}
        onTouchStart={this.onTouchStart}
      />
    )
  }
}

const mstp = (state: AppState): StateProps => ({
  telegrafConfig: state.telegrafs.currentConfig.item,
  status: state.telegrafs.currentConfig.status,
})

const mdtp: DispatchProps = {
  getTelegrafConfigToml: getTelegrafConfigToml,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(TelegrafConfig))
