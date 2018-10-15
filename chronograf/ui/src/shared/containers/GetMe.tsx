import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {ErrorHandling} from 'src/shared/decorators/errors'
import {getMe} from 'src/shared/actions/v2/me'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectDispatchProps {
  getMe: typeof getMe
}

interface State {
  ready: boolean
}

type Props = ConnectDispatchProps & PassedInProps

@ErrorHandling
class GetMe extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      ready: false,
    }
  }

  public render() {
    if (this.state.ready) {
      return this.props.children && React.cloneElement(this.props.children)
    }

    return <div className="page-spinner" />
  }

  public async componentDidMount() {
    await this.props.getMe()
    this.setState({ready: true})
  }
}

const mdtp = {
  getMe,
}

export default connect<{}, ConnectDispatchProps, PassedInProps>(null, mdtp)(
  GetMe
)
