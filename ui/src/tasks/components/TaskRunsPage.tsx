// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router-dom'

// Components
import {Page, IconFont, Sort} from '@influxdata/clockface'
import TaskRunsList from 'src/tasks/components/TaskRunsList'
import CloudUpgradeButton from 'src/shared/components/CloudUpgradeButton'

// Types
import {AppState, RemoteDataState, Task, Run} from 'src/types'
import {
  SpinnerContainer,
  TechnoSpinner,
  Button,
  ComponentColor,
} from '@influxdata/clockface'

// Actions
import {getRuns, runTask} from 'src/tasks/actions/thunks'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Types
import {SortTypes} from 'src/shared/utils/sort'

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

interface State {
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof Run

class TaskRunsPage extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      sortKey: 'scheduledFor',
      sortDirection: Sort.Descending,
      sortType: SortTypes.Date,
    }
  }

  public render() {
    const {params, runs} = this.props
    const {sortKey, sortDirection, sortType} = this.state

    return (
      <SpinnerContainer
        loading={this.props.runStatus}
        spinnerComponent={<TechnoSpinner />}
      >
        <Page titleTag={pageTitleSuffixer(['Task Runs'])}>
          <Page.Header fullWidth={false}>
            <Page.Title title={this.title} />
            <CloudUpgradeButton />
          </Page.Header>
          <Page.ControlBar fullWidth={false}>
            <Page.ControlBarLeft>
              <Button
                onClick={this.handleEditTask}
                text="Edit Task"
                color={ComponentColor.Primary}
              />
            </Page.ControlBarLeft>
            <Page.ControlBarRight>
              <Button
                onClick={this.handleRunTask}
                text="Run Task"
                icon={IconFont.Play}
              />
            </Page.ControlBarRight>
          </Page.ControlBar>
          <Page.Contents fullWidth={false} scrollable={true}>
            <TaskRunsList
              taskID={params.id}
              runs={runs}
              sortKey={sortKey}
              sortDirection={sortDirection}
              sortType={sortType}
              onClickColumn={this.handleClickColumn}
            />
          </Page.Contents>
        </Page>
      </SpinnerContainer>
    )
  }

  public componentDidMount() {
    this.props.getRuns(this.props.params.id)
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    let sortType = SortTypes.String

    if (sortKey !== 'status') {
      sortType = SortTypes.Date
    }

    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private get title() {
    const {currentTask} = this.props

    if (currentTask) {
      return `${currentTask.name} - Runs`
    }
    return 'Runs'
  }

  private handleRunTask = () => {
    const {onRunTask, params, getRuns} = this.props
    onRunTask(params.id)
    getRuns(params.id)
  }

  private handleEditTask = () => {
    const {
      router,
      currentTask,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tasks/${currentTask.id}`)
  }
}

const mstp = (state: AppState): StateProps => {
  const {runs, runStatus, currentTask} = state.resources.tasks

  return {
    runs,
    runStatus,
    currentTask,
  }
}

const mdtp: DispatchProps = {
  getRuns: getRuns,
  onRunTask: runTask,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<OwnProps>(TaskRunsPage))
