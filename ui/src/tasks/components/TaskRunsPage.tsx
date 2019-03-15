// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import TaskRunsList from 'src/tasks/components/TaskRunsList'

// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'
import {Run} from '@influxdata/influx'
import {SpinnerContainer, TechnoSpinner, Button} from '@influxdata/clockface'

// Actions
import {getRuns, runTask} from 'src/tasks/actions/v2'
import {IconFont} from 'src/clockface'

interface OwnProps {
  params: {id: string}
}

interface DispatchProps {
  getRuns: typeof getRuns
  onRunTask: typeof runTask
}

interface StateProps {
  runs: Run[]
  runStatus: RemoteDataState
}

type Props = OwnProps & DispatchProps & StateProps

class TaskRunsPage extends PureComponent<Props> {
  public render() {
    const {params, runs} = this.props
    return (
      <SpinnerContainer
        loading={this.props.runStatus}
        spinnerComponent={<TechnoSpinner />}
      >
        <Page titleTag="Runs">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Runs" />
            </Page.Header.Left>
            <Page.Header.Right>
              <Button
                onClick={this.handleRunTask}
                text="Run Task"
                icon={IconFont.Play}
              />
            </Page.Header.Right>
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <TaskRunsList taskID={params.id} runs={runs} />
            </div>
          </Page.Contents>
        </Page>
      </SpinnerContainer>
    )
  }

  public componentDidMount() {
    this.props.getRuns(this.props.params.id)
  }

  private handleRunTask = async () => {
    const {onRunTask, params, getRuns} = this.props
    await onRunTask(params.id)
    getRuns(params.id)
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    tasks: {runs, runStatus},
  } = state
  return {runs, runStatus}
}

const mdtp: DispatchProps = {getRuns: getRuns, onRunTask: runTask}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TaskRunsPage)
