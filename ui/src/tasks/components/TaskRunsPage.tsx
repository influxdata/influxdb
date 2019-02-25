// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Types
import {Page} from 'src/pageLayout'
import TaskRunsList from 'src/tasks/components/TaskRunsList'
import {AppState} from 'src/types/v2'

// Actions
import {getRuns} from 'src/tasks/actions/v2'
import {Run} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

interface PassedInProps {
  params: {id: string}
}

interface ConnectedDispatchProps {
  getRuns: typeof getRuns
}

interface ConnectedStateProps {
  runs: Run[]
}

interface State {
  loading: RemoteDataState
}

type Props = PassedInProps & ConnectedDispatchProps & ConnectedStateProps

class TaskRunsPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {loading: RemoteDataState.NotStarted}
  }
  public render() {
    const {params, runs} = this.props

    if (this.state.loading !== RemoteDataState.Done) {
      return (
        <>
          <Page titleTag="Runs">
            <Page.Header fullWidth={false}>
              <Page.Header.Left>
                <Page.Title title="Runs" />
              </Page.Header.Left>
              <Page.Header.Right />
            </Page.Header>
            <Page.Contents fullWidth={false} scrollable={true}>
              <div className="col-xs-12">
                <TaskRunsList taskID={params.id} runs={[]} />
              </div>
            </Page.Contents>
          </Page>
        </>
      )
    }
    return (
      <>
        <Page titleTag="Runs">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Runs" />
            </Page.Header.Left>
            <Page.Header.Right />
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <TaskRunsList taskID={params.id} runs={runs} />
            </div>
          </Page.Contents>
        </Page>
      </>
    )
  }
  public async componentDidMount() {
    this.setState({loading: RemoteDataState.Loading})
    try {
      await this.props.getRuns(this.props.params.id)
      this.setState({loading: RemoteDataState.Done})
    } catch (e) {
      this.setState({loading: RemoteDataState.Error})
    }
  }
}

const mstp = (state: AppState): ConnectedStateProps => {
  const {
    tasks: {runs},
  } = state
  return {runs}
}

const mdtp: ConnectedDispatchProps = {getRuns: getRuns}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(
  mstp,
  mdtp
)(TaskRunsPage)
