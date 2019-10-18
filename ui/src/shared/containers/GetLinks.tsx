// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

// Actions
import {getLinks} from 'src/shared/actions/links'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectDispatchProps {
  getLinks: typeof getLinks
}

interface State {
  loading: RemoteDataState
}

type Props = ConnectDispatchProps & PassedInProps

@ErrorHandling
class GetLinks extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }

  public componentDidMount() {
    this.props.getLinks()
    this.setState({loading: RemoteDataState.Done})
  }

  public render() {
    const {loading} = this.state

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {this.props.children && React.cloneElement(this.props.children)}
      </SpinnerContainer>
    )
  }
}

const mdtp = {
  getLinks,
}

export default connect<{}, ConnectDispatchProps, PassedInProps>(
  null,
  mdtp
)(GetLinks)
