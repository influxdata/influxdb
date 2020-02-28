// Libraries
import {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import auth0js from 'auth0-js'

// APIs
import {postSignout} from 'src/client'

// Constants
import {CLOUD, CLOUD_URL, CLOUD_LOGOUT_PATH} from 'src/shared/constants'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Utils
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// TODO: these are filler properties that will be populated on IDPE in a later iteration
const auth0 = new auth0js.WebAuth({
  domain: 'www.influxdata.com',
  clientID: 'abc123',
})

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
    if (CLOUD && isFlagEnabled('IDPELoginPage')) {
      auth0.logout({})
      return
    }
    if (CLOUD) {
      window.location.href = `${CLOUD_URL}${CLOUD_LOGOUT_PATH}`
      return
    } else {
      const resp = await postSignout({})

      if (resp.status !== 204) {
        throw new Error(resp.data.message)
      }

      this.props.router.push(`/signin`)
    }
  }
}

export default withRouter<Props>(Logout)
