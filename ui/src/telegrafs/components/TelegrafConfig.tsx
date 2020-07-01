// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import Editor from 'src/shared/components/TomlMonacoEditor'
import {RemoteDataState} from '@influxdata/clockface'

// Actions
import {getTelegrafConfigToml} from 'src/telegrafs/actions/thunks'

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
export class TelegrafConfig extends PureComponent<
  Props & RouteComponentProps<{orgID: string; id: string}>
> {
  public componentDidMount() {
    const {
      match: {
        params: {id},
      },
      getTelegrafConfigToml,
    } = this.props
    getTelegrafConfigToml(id)
  }

  public render() {
    return <>{this.overlayBody}</>
  }

  private get overlayBody(): JSX.Element {
    const {telegrafConfig} = this.props
    return <Editor script={telegrafConfig} readOnly />
  }
}

const mstp = ({resources}: AppState): StateProps => ({
  telegrafConfig: resources.telegrafs.currentConfig.item,
  status: resources.telegrafs.currentConfig.status,
})

const mdtp: DispatchProps = {
  getTelegrafConfigToml: getTelegrafConfigToml,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(TelegrafConfig))
