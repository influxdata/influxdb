// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

// Actions
import {getMe} from 'src/shared/actions/me'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectDispatchProps {
  getMe: typeof getMe
}

interface State {
  loading: RemoteDataState
}

type Props = ConnectDispatchProps & PassedInProps

@ErrorHandling
class GetMe extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }

  public render() {
    const {loading} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {this.props.children && React.cloneElement(this.props.children)}
      </SpinnerContainer>
    )
  }

  public componentDidMount() {
    this.props.getMe()
    this.setState({loading: RemoteDataState.Done})
  }
}

const mdtp = {
  getMe,
}

export default connect<{}, ConnectDispatchProps, PassedInProps>(
  null,
  mdtp
)(GetMe)
