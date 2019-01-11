import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {RemoteDataState} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {getLinks} from 'src/shared/actions/links'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectDispatchProps {
  getLinks: typeof getLinks
}

interface State {
  ready: RemoteDataState
}

type Props = ConnectDispatchProps & PassedInProps

@ErrorHandling
class GetLinks extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      ready: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    await this.props.getLinks()
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
  getLinks,
}

export default connect<{}, ConnectDispatchProps, PassedInProps>(
  null,
  mdtp
)(GetLinks)
