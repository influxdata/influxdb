// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {
  EmptyState,
  TechnoSpinner,
  SpinnerContainer,
} from '@influxdata/clockface'

// APIs
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Label} from 'src/types/labels'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: (labels: Label[]) => JSX.Element
}

interface State {
  labels: Label[]
  loading: RemoteDataState
}

@ErrorHandling
class FetchLabels extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      labels: [],
      loading: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    const labels = await client.labels.getAll()
    this.setState({
      loading: RemoteDataState.Done,
      labels: _.orderBy(labels, ['name']),
    })
  }

  public render() {
    const {loading} = this.state

    if (loading === RemoteDataState.Error) {
      return (
        <EmptyState>
          <EmptyState.Text text="Could not load labels" />
        </EmptyState>
      )
    }

    return (
      <SpinnerContainer loading={loading} spinnerComponent={<TechnoSpinner />}>
        {this.props.children(this.state.labels)}
      </SpinnerContainer>
    )
  }
}

export default FetchLabels
