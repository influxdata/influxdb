// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import Editor from 'src/shared/components/TomlMonacoEditor'

// Actions
import {getTelegrafConfigToml} from 'src/telegrafs/actions/thunks'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

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

const mstp = ({resources}: AppState) => ({
  telegrafConfig: resources.telegrafs.currentConfig.item,
  status: resources.telegrafs.currentConfig.status,
})

const mdtp = {
  getTelegrafConfigToml: getTelegrafConfigToml,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(TelegrafConfig))
