// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import auth0js from 'auth0-js'

import {client} from 'src/utils/api'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {sessionTimedOut} from 'src/shared/copy/notifications'
import {CLOUD, CLOUD_SIGNIN_PATHNAME} from 'src/shared/constants'

// Types
import {RemoteDataState} from 'src/types'

// Utils
import {getAuth0Config} from 'src/authorizations/apis'

interface State {
  loading: RemoteDataState
}

interface OwnProps {
  children: ReactElement<any>
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = OwnProps & WithRouterProps & DispatchProps

const FETCH_WAIT = 60000

@ErrorHandling
export class Signin extends PureComponent<Props, State> {
  public state: State = {loading: RemoteDataState.NotStarted}

  private hasMounted = false
  private intervalID: NodeJS.Timer

  public async componentDidMount() {
    this.hasMounted = true
    this.setState({loading: RemoteDataState.Loading})

    await this.checkForLogin()

    if (this.hasMounted) {
      this.setState({loading: RemoteDataState.Done})
      this.intervalID = setInterval(() => {
        this.checkForLogin()
      }, FETCH_WAIT)
    }
  }

  public componentWillUnmount() {
    clearInterval(this.intervalID)
    this.hasMounted = false
  }

  public render() {
    const {loading} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {this.props.children && React.cloneElement(this.props.children)}
      </SpinnerContainer>
    )
  }

  private checkForLogin = async () => {
    try {
      await client.users.me()
    } catch (error) {
      const {
        location: {pathname},
      } = this.props

      clearInterval(this.intervalID)

      const config = await getAuth0Config()
      if (CLOUD && config.socialSignUpOn) {
        // The responseType is arbitrary as it needs to be a non-empty, non "code" value:
        // https://auth0.github.io/auth0.js/web-auth_index.js.html#line564
        const auth0 = new auth0js.WebAuth({
          domain: config.domain,
          clientID: config.clientID,
          state: config.state,
          redirectUri: config.redirectURL,
          responseType: 'token',
        })

        auth0.checkSession({}, (error) => {
            if (error) {
              this.setState({loading: RemoteDataState.Error})
              console.error(error)
              return
            }
        })
      }

      // TODO: add returnTo to CLOUD signin
      if (CLOUD) {
        window.location.pathname = CLOUD_SIGNIN_PATHNAME

        throw error
      }

      if (pathname.startsWith('/signin')) {
        return
      }

      let returnTo = ''

      if (pathname !== '/') {
        returnTo = `?returnTo=${pathname}`
        this.props.notify(sessionTimedOut())
      }

      this.props.router.replace(`/signin${returnTo}`)
    }
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect(
  null,
  mdtp
)(withRouter(Signin))
