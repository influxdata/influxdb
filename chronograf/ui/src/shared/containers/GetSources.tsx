// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// APIs
import {getSourcesAsync} from 'src/shared/actions/sources'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: React.ReactElement<any>
  getSources: typeof getSourcesAsync
}

interface State {
  ready: boolean
}

@ErrorHandling
export class GetSources extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      ready: false,
    }
  }

  public async componentDidMount() {
    await this.props.getSources()
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
  getSources: getSourcesAsync,
}

export default connect(null, mdtp)(GetSources)
