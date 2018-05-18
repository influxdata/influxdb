import {PureComponent, ReactChildren} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import {Source} from 'src/types'

import * as actions from 'src/shared/actions/services'

interface Props {
  sources: Source[]
  children: ReactChildren
  fetchServicesAsync: actions.FetchServicesAsync
}

export class CheckServices extends PureComponent<Props & WithRouterProps> {
  public async componentDidMount() {
    const source = this.props.sources.find(
      s => s.id === this.props.params.sourceID
    )

    if (!source) {
      return
    }

    await this.props.fetchServicesAsync(source)
  }

  public render() {
    return this.props.children
  }
}

const mdtp = {
  fetchServicesAsync: actions.fetchServicesAsync,
}

const mstp = ({sources}) => ({sources})

export default connect(mstp, mdtp)(withRouter(CheckServices))
