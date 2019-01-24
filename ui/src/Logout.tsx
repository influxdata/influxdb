// Libraries
import {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// APIs
import {client} from 'src/utils/api'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

type Props = WithRouterProps

@ErrorHandling
export class Logout extends PureComponent<Props> {
  public componentDidMount() {
    this.handleSignOut()
  }

  public render() {
    return null
  }

  private handleSignOut = async () => {
    await client.auth.signout()

    this.props.router.push(`/signin`)
  }
}

export default withRouter<Props>(Logout)
