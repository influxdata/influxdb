// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Types
import {RemoteDataState} from 'src/types'

// Actions
import {getLinks} from 'src/shared/actions/links'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  children: React.ReactElement<any>
}

interface State {
  loading: RemoteDataState
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps & OwnProps

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

const connector = connect(null, mdtp)

export default connector(GetLinks)
