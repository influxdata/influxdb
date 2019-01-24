// Libraries
import React, {PureComponent} from 'react'

// Components
import {EmptyState} from 'src/clockface'

// APIs
import {client} from 'src/utils/api'

// Types
import {RemoteDataState} from 'src/types'
import {Label} from 'src/api'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: (labels: Label[]) => JSX.Element
}

interface State {
  labels: Label[]
  ready: RemoteDataState
}

@ErrorHandling
class FetchLabels extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      labels: [],
      ready: RemoteDataState.NotStarted,
    }
  }

  public async componentDidMount() {
    const labels = await client.labels.getAll()
    this.setState({ready: RemoteDataState.Done, labels})
  }

  public render() {
    if (this.state.ready === RemoteDataState.Error) {
      return (
        <EmptyState>
          <EmptyState.Text text="Could not load labels" />
        </EmptyState>
      )
    }

    if (this.state.ready !== RemoteDataState.Done) {
      return <div className="page-spinner" />
    }

    return this.props.children(this.state.labels)
  }
}

export default FetchLabels
