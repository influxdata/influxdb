// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

import {RemoteDataState} from 'src/types'

import {readSources} from 'src/sources/actions'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: React.ReactElement<any>
  onReadSources: typeof readSources
}

interface State {
  ready: RemoteDataState
}

@ErrorHandling
export class GetSources extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      ready: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    await this.props.onReadSources()

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
  onReadSources: readSources,
}

export default connect(
  null,
  mdtp
)(GetSources)
