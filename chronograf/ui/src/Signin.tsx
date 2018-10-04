// Libraries
import React, {ReactElement, PureComponent} from 'react'
import {connect} from 'react-redux'

// APIs
import {trySources} from 'src/onboarding/apis'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import SigninPage from 'src/onboarding/containers/SigninPage'
import Notifications from 'src/shared/components/notifications/Notifications'

// Types
import {RemoteDataState} from 'src/types'
import {Links} from 'src/types/v2/links'

interface State {
  loading: RemoteDataState
  isSourcesAllowed: boolean
}

interface Props {
  links: Links
  children: ReactElement<any>
}

@ErrorHandling
export class Signin extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
      isSourcesAllowed: false,
    }
  }

  public async componentDidMount() {
    const {links} = this.props
    const isSourcesAllowed = await trySources(links.sources)
    this.setState({loading: RemoteDataState.Done, isSourcesAllowed})
  }

  public render() {
    const {isSourcesAllowed} = this.state

    if (this.isLoading) {
      return <div className="page-spinner" />
    }
    if (!isSourcesAllowed) {
      return (
        <div className="chronograf-root">
          <Notifications inPresentationMode={true} />
          <SigninPage />
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
}

const mstp = ({links}) => ({
  links,
})

export default connect(mstp)(Signin)
