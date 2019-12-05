// Libraries
import React, {PureComponent, Suspense} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
const Editor = React.lazy(() =>
  import('react-codemirror2').then(module => ({default: module.Controlled}))
)
const MonacoEditor = React.lazy(() =>
  import('src/shared/components/TomlMonacoEditor')
)
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

const spinner = <div className="time-machine-editor--loading" />

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
      <Suspense fallback={spinner}>
        <FeatureFlag name="monacoEditor">
          <MonacoEditor script={telegrafConfig} />
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
      </Suspense>
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
