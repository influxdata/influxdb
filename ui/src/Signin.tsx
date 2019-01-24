// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

import {client} from 'src/utils/api'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {sessionTimedOut} from 'src/shared/copy/notifications'

// Types
import {RemoteDataState} from 'src/types'

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
  private intervalID: NodeJS.Timer
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    this.setState({loading: RemoteDataState.Done})
    this.checkForLogin()
    this.intervalID = setInterval(this.checkForLogin, FETCH_WAIT)
  }

  public componentWillUnmount() {
    clearInterval(this.intervalID)
  }

  public render() {
    if (this.isLoading) {
      return <div className="page-spinner" />
    }

    return this.props.children && React.cloneElement(this.props.children)
  }

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
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

      if (pathname.startsWith('/signin')) {
        return
      }

      let returnTo = ''

      if (pathname !== '/') {
        returnTo = `?returnTo=${pathname}`
        this.props.notify(sessionTimedOut())
      }

      this.props.router.push(`/signin${returnTo}`)
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
