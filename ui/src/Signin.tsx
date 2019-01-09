// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// APIs
import {trySources} from 'src/onboarding/apis'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {getMe} from 'src/shared/apis/v2/user'

// Utils
import {AuthContext} from 'src/utils/auth'

// Types
import {RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
  isUserSignedIn: boolean
}

interface OwnProps {
  children: ReactElement<any>
}

type Props = OwnProps & WithRouterProps

const FETCH_WAIT = 60000

@ErrorHandling
export class Signin extends PureComponent<Props, State> {
  private intervalID: NodeJS.Timer
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
      isUserSignedIn: false,
    }
  }

  public async componentDidMount() {
    const isSourcesAllowed = await trySources()
    const isUserSignedIn = isSourcesAllowed
    this.setState({loading: RemoteDataState.Done, isUserSignedIn})
    if (!isUserSignedIn) {
      this.props.router.push('/signin')
    }
  }

  public render() {
    if (this.isLoading) {
      return <div className="page-spinner" />
    }

    return (
      <AuthContext.Provider value={{onSignInUser: this.handleSignInUser}}>
        {this.props.children && React.cloneElement(this.props.children)}
      </AuthContext.Provider>
    )
  }

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    )
  }

  private handleSignInUser = () => {
    this.intervalID = setInterval(this.checkForLogin, FETCH_WAIT)
    this.setState({isUserSignedIn: true})
    this.props.router.push('/dashboards')
  }

  private checkForLogin = async () => {
    try {
      await getMe()
    } catch (error) {
      clearInterval(this.intervalID)
      this.setState({isUserSignedIn: false})
      this.props.router.push('/signin')
    }
  }
}

export default withRouter(Signin)
