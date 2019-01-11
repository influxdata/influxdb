import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {RemoteDataState} from 'src/types'

import {getOrganizations} from 'src/organizations/actions'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectDispatchProps {
  getOrganizations: typeof getOrganizations
}

type Props = ConnectDispatchProps & PassedInProps

interface State {
  ready: RemoteDataState
}

@ErrorHandling
class GetOrganizations extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      ready: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    await this.props.getOrganizations()
    this.setState({ready: RemoteDataState.Done})
  }

  public render() {
    if (this.state.ready !== RemoteDataState.Done) {
      return <div className="page-spinner" />
    }

    return this.props.children && React.cloneElement(this.props.children)
  }
}

const mdtp = {
  getOrganizations,
}

export default connect<{}, ConnectDispatchProps, PassedInProps>(
  null,
  mdtp
)(GetOrganizations)
