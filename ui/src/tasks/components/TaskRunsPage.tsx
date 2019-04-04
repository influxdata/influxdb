// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import TaskRunsList from 'src/tasks/components/TaskRunsList'
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

// Types
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'
import {Run as APIRun, Task} from '@influxdata/influx'
import {SpinnerContainer, TechnoSpinner, Button} from '@influxdata/clockface'

// Actions
import {getRuns, runTask} from 'src/tasks/actions'
import {IconFont} from 'src/clockface'

export interface Run extends APIRun {
  duration: string
}

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
  currentTask: Task
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
              <PageTitleWithOrg title={this.title} />
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

  private get title() {
    const {currentTask} = this.props

    if (currentTask) {
      return `${currentTask.name} - Runs`
    }
    return 'Runs'
  }

  private handleRunTask = async () => {
    const {onRunTask, params, getRuns} = this.props
    await onRunTask(params.id)
    getRuns(params.id)
  }
}

const mstp = (state: AppState): StateProps => {
  const {tasks} = state

  return {
    runs: tasks.runs,
    runStatus: tasks.runStatus,
    currentTask: tasks.currentTask,
  }
}

const mdtp: DispatchProps = {getRuns: getRuns, onRunTask: runTask}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TaskRunsPage)
