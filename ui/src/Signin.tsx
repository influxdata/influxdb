// Libraries
import React, {ReactElement, PureComponent} from 'react'

// APIs
import {trySources} from 'src/onboarding/apis'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SigninPage from 'src/onboarding/containers/SigninPage'
import Notifications from 'src/shared/components/notifications/Notifications'

// Types
import {RemoteDataState} from 'src/types'

interface State {
  loading: RemoteDataState
  isUserSignedIn: boolean
}

interface Props {
  children: ReactElement<any>
}

@ErrorHandling
export class Signin extends PureComponent<Props, State> {
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
  }

  public render() {
    const {isUserSignedIn} = this.state

    if (this.isLoading) {
      return <div className="page-spinner" />
    }
    if (!isUserSignedIn) {
      return (
        <div className="chronograf-root">
          <Notifications inPresentationMode={true} />
          <SigninPage onSignInUser={this.handleSignInUser} />
        </div>
      )
    } else {
      return this.props.children && React.cloneElement(this.props.children)
    }
  }

  private get isLoading(): boolean {
    const {loading} = this.state
    return (
      loading === RemoteDataState.Loading ||
      loading === RemoteDataState.NotStarted
    )
  }

  private handleSignInUser = () => {
    this.setState({isUserSignedIn: true})
  }
}

export default Signin
