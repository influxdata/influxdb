import _ from 'lodash'
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {getLinksAsync} from 'src/shared/actions/links'

interface PassedInProps {
  children: React.ReactElement<any>
}

interface ConnectStateProps {
  links: object
}

interface ConnectDispatchProps {
  getLinks: typeof getLinksAsync
}

interface State {
  ready: boolean
}

type Props = ConnectStateProps & ConnectDispatchProps & PassedInProps

class GetLinks extends PureComponent<Props, State> {
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
    await this.props.getLinks()
    this.setState({ready: true})
  }
}

const mstp = ({links}) => {
  return {
    links,
  }
}

const mdtp = {
  getLinks: getLinksAsync,
}

export default connect<ConnectStateProps, ConnectDispatchProps, PassedInProps>(
  mstp,
  mdtp
)(GetLinks)
