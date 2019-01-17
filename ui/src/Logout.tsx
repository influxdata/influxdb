// Libraries
import {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// APIs
import {logout} from 'src/me/apis'

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
    await logout()
    const {query} = this.props.location
    let returnTo = ''

    if (query && query.returnTo) {
      returnTo = `?returnTo=${query.returnTo}`
    }

    this.props.router.push(`/signin${returnTo}`)
  }
}

export default withRouter<Props>(Logout)
