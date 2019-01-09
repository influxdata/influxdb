// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// APIs
import {trySources} from 'src/onboarding/apis'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {getMe} from 'src/shared/apis/v2/user'

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
    this.checkForLogin()
    this.intervalID = setInterval(this.checkForLogin, FETCH_WAIT)
    if (!isUserSignedIn) {
      this.props.router.push('/signin')
    }
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
      await getMe()
    } catch (error) {
      clearInterval(this.intervalID)
      this.setState({isUserSignedIn: false})
      this.props.router.push('/signin')
    }
  }
}

export default withRouter(Signin)
