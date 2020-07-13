// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps, matchPath} from 'react-router-dom'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import Editor from 'src/shared/components/TomlMonacoEditor'

// Actions
import {getTelegrafConfigToml} from 'src/telegrafs/actions/thunks'

// Types
import {AppState} from 'src/types'

type Params = {orgID: string; id: string}
type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

@ErrorHandling
export class TelegrafConfig extends PureComponent<Props & RouteComponentProps> {
  public componentDidMount() {
    const match = matchPath<Params>(this.props.history.location.pathname, {
      path: '/orgs/:orgID/load-data/telegrafs/:id/view',
      exact: true,
      strict: false,
    })
    const id = match.params.id
    this.props.getTelegrafConfigToml(id)
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
  getTelegrafConfigToml,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(TelegrafConfig))
