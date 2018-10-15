import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {getOrganizations} from 'src/shared/actions/v2/orgs'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectDispatchProps {
  getOrganizations: typeof getOrganizations
}

type Props = ConnectDispatchProps & PassedInProps

interface State {
  ready: boolean
}

@ErrorHandling
class GetOrganizations extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      ready: false,
    }
  }

  public async componentDidMount() {
    await this.props.getOrganizations()
    this.setState({ready: true})
  }

  public render() {
    if (this.state.ready) {
      return this.props.children && React.cloneElement(this.props.children)
    }

    return <div className="page-spinner" />
  }
}

const mdtp = {
  getOrganizations,
}

export default connect<{}, ConnectDispatchProps, PassedInProps>(null, mdtp)(
  GetOrganizations
)
