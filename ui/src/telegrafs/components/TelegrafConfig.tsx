// Libraries
import React, {PureComponent} from 'react'
import Loadable from 'react-loadable'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
const spinner = <div className="time-machine-editor--loading" />
const Editor = Loadable({
  loader: () => import('react-codemirror2').then(module => module.Controlled),
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
import {FeatureFlag, isFlagEnabled} from 'src/shared/utils/featureFlag'
import TelegrafEditor from 'src/dataLoaders/components/TelegrafEditor'

// Actions
import {getTelegrafConfigToml} from 'src/telegrafs/actions/thunks'
import {loadTelegrafConfig} from 'src/dataLoaders/actions/telegrafEditor'

// Types
import {AppState} from 'src/types'

interface DispatchProps {
  getTelegrafConfigToml: typeof getTelegrafConfigToml
  loadTelegrafConfig: typeof loadTelegrafConfig
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
      loadTelegrafConfig,
    } = this.props

    if (isFlagEnabled('telegrafEditor')) {
      loadTelegrafConfig(id)
    } else {
      getTelegrafConfigToml(id)
    }
  }

  public render() {
    if (isFlagEnabled('telegrafEditor')) {
      return <>{this.editorBody}</>
    }

    return <>{this.overlayBody}</>
  }

  private onBeforeChange = () => {}
  private onTouchStart = () => {}

  private get editorBody(): JSX.Element {
    return <TelegrafEditor />
  }

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

const mstp = ({resources}: AppState): StateProps => ({
  telegrafConfig: resources.telegrafs.currentConfig.item,
  status: resources.telegrafs.currentConfig.status,
})

const mdtp: DispatchProps = {
  getTelegrafConfigToml: getTelegrafConfigToml,
  loadTelegrafConfig: loadTelegrafConfig,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(TelegrafConfig))
