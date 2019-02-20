// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

// Actions
import {readSources} from 'src/sources/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: React.ReactElement<any>
  onReadSources: typeof readSources
}

interface State {
  loading: RemoteDataState
}

@ErrorHandling
export class GetSources extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    await this.props.onReadSources()

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
  onReadSources: readSources,
}

export default connect(
  null,
  mdtp
)(GetSources)
