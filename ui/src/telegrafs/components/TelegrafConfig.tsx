// Libraries
import React, {PureComponent} from 'react'
import Loadable from 'react-loadable'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
const spinner = <div className="time-machine-editor--loading" />
const Editor = Loadable({
  loader: () =>
    import('react-codemirror2').then(module => ({default: module.Controlled})),
  loading() {
    return spinner
  },
})
const MonacoEditor = Loadable({
  loader: () => import('src/shared/components/TomlMonacoEditor'),
  loading() {
    return spinner
  },
})
import {RemoteDataState} from '@influxdata/clockface'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

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
    const {telegrafConfig} = this.props
    return (
      <>
        <FeatureFlag name="monacoEditor">
          <MonacoEditor script={telegrafConfig} readOnly />
        </FeatureFlag>
        <FeatureFlag name="monacoEditor" equals={false}>
          <Editor
            autoFocus={true}
            autoCursor={true}
            value={telegrafConfig}
            options={{
              tabindex: 1,
              mode: 'toml',
              readOnly: true,
              lineNumbers: true,
              theme: 'time-machine',
            }}
            onBeforeChange={this.onBeforeChange}
            onTouchStart={this.onTouchStart}
          />
        </FeatureFlag>
      </>
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
