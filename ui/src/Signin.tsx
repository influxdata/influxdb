// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {getMe} from 'src/shared/apis/v2/user'

// Types
import {RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
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
      await getMe()
    } catch (error) {
      clearInterval(this.intervalID)
      this.props.router.push('/signin')
    }
  }
}

export default withRouter(Signin)
