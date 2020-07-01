// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {Switch, Route, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'

import {client} from 'src/utils/api'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'
import GetMe from 'src/shared/containers/GetMe'

// Utils
import {
  getFromLocalStorage,
  removeFromLocalStorage,
  setToLocalStorage,
} from 'src/localStorage'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {sessionTimedOut} from 'src/shared/copy/notifications'
import {CLOUD, CLOUD_SIGNIN_PATHNAME} from 'src/shared/constants'

// Types
import {RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
  auth: boolean
}

interface OwnProps {
  children: ReactElement<any>
}

interface DispatchProps {
  notify: typeof notifyAction
}

type Props = OwnProps & RouteComponentProps & DispatchProps

const FETCH_WAIT = 60000

@ErrorHandling
export class Signin extends PureComponent<Props, State> {
  public state: State = {loading: RemoteDataState.NotStarted, auth: false}

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
    const {loading, auth} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {auth && (
          <Switch>
            <Route render={props => <GetMe {...props} />} />
          </Switch>
        )}
      </SpinnerContainer>
    )
  }

  private checkForLogin = async () => {
    try {
      await client.users.me()

      this.setState({auth: true})
      const redirectIsSet = !!getFromLocalStorage('redirectTo')
      if (redirectIsSet) {
        removeFromLocalStorage('redirectTo')
      }
    } catch (error) {
      this.setState({auth: false})
      const {
        location: {pathname},
      } = this.props

      clearInterval(this.intervalID)

      if (CLOUD) {
        const url = new URL(
          `${window.location.origin}${CLOUD_SIGNIN_PATHNAME}?redirectTo=${window.location.href}`
        )
        setToLocalStorage('redirectTo', window.location.href)
        window.location.href = url.href
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

      this.props.history.replace(`/signin${returnTo}`)
    }
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
}

export default connect(null, mdtp)(Signin)
