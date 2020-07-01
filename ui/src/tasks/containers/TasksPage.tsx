// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FilterList from 'src/shared/components/FilterList'
import GetResources from 'src/resources/components/GetResources'
import GetAssetLimits from 'src/cloud/components/GetAssetLimits'
import AssetLimitAlert from 'src/cloud/components/AssetLimitAlert'
import TaskExportOverlay from 'src/tasks/components/TaskExportOverlay'
import TaskImportOverlay from 'src/tasks/components/TaskImportOverlay'
import TaskImportFromTemplateOverlay from 'src/tasks/components/TaskImportFromTemplateOverlay'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Actions
import {
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  cloneTask,
  addTaskLabel,
  runTask,
} from 'src/tasks/actions/thunks'

import {
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
} from 'src/tasks/actions/creators'

import {
  checkTaskLimits as checkTasksLimitsAction,
  LimitStatus,
} from 'src/cloud/actions/limits'

// Types
import {AppState, Task, RemoteDataState, ResourceType} from 'src/types'
import {RouteComponentProps} from 'react-router-dom'
import {Sort} from '@influxdata/clockface'
import {SortTypes} from 'src/shared/utils/sort'
import {extractTaskLimits} from 'src/cloud/utils/limits'
import {TaskSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

// Selectors
import {getAll} from 'src/resources/selectors'

interface ConnectedDispatchProps {
  updateTaskStatus: typeof updateTaskStatus
  updateTaskName: typeof updateTaskName
  deleteTask: typeof deleteTask
  cloneTask: typeof cloneTask
  setSearchTerm: typeof setSearchTermAction
  setShowInactive: typeof setShowInactiveAction
  onAddTaskLabel: typeof addTaskLabel
  onRunTask: typeof runTask
  checkTaskLimits: typeof checkTasksLimitsAction
}

interface ConnectedStateProps {
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  status: RemoteDataState
  limitStatus: LimitStatus
}

type Props = ConnectedDispatchProps &
  ConnectedStateProps &
  RouteComponentProps<{orgID: string}>

interface State {
  isImporting: boolean
  taskLabelsEdit: Task
  sortKey: TaskSortKey
  sortDirection: Sort
  sortType: SortTypes
}

const Filter = FilterList<Task>()

@ErrorHandling
class TasksPage extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    props.setSearchTerm('')
    if (!props.showInactive) {
      props.setShowInactive()
    }

    this.state = {
      isImporting: false,
      taskLabelsEdit: null,
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render(): JSX.Element {
    const {sortKey, sortDirection, sortType} = this.state
    const {
      setSearchTerm,
      updateTaskName,
      searchTerm,
      setShowInactive,
      showInactive,
      onAddTaskLabel,
      onRunTask,
      checkTaskLimits,
      limitStatus,
    } = this.props

    return (
      <>
        <Page titleTag={pageTitleSuffixer(['Tasks'])}>
          <TasksHeader
            onCreateTask={this.handleCreateTask}
            setShowInactive={setShowInactive}
            showInactive={showInactive}
            onImportTask={this.summonImportOverlay}
            onImportFromTemplate={this.summonImportFromTemplateOverlay}
            limitStatus={limitStatus}
            searchTerm={searchTerm}
            setSearchTerm={setSearchTerm}
            sortKey={sortKey}
            sortDirection={sortDirection}
            sortType={sortType}
            onSort={this.handleSort}
          />
          <Page.Contents fullWidth={false} scrollable={true}>
            <GetResources resources={[ResourceType.Tasks, ResourceType.Labels]}>
              <GetAssetLimits>
                <AssetLimitAlert
                  resourceName="tasks"
                  limitStatus={limitStatus}
                />
                <Filter
                  list={this.filteredTasks}
                  searchTerm={searchTerm}
                  searchKeys={['name', 'labels[].name']}
                >
                  {ts => (
                    <TasksList
                      searchTerm={searchTerm}
                      tasks={ts}
                      totalCount={this.totalTaskCount}
                      onActivate={this.handleActivate}
                      onDelete={this.handleDelete}
                      onCreate={this.handleCreateTask}
                      onClone={this.handleClone}
                      onAddTaskLabel={onAddTaskLabel}
                      onRunTask={onRunTask}
                      onFilterChange={setSearchTerm}
                      onUpdate={updateTaskName}
                      onImportTask={this.summonImportOverlay}
                      onImportFromTemplate={
                        this.summonImportFromTemplateOverlay
                      }
                      sortKey={sortKey}
                      sortDirection={sortDirection}
                      sortType={sortType}
                      checkTaskLimits={checkTaskLimits}
                    />
                  )}
                </Filter>
                {this.hiddenTaskAlert}
              </GetAssetLimits>
            </GetResources>
          </Page.Contents>
        </Page>
        <Switch>
          <Route
            path="/orgs/:orgID/tasks/:id/export"
            component={TaskExportOverlay}
          />
          <Route
            path="/orgs/:orgID/tasks/import"
            component={TaskImportOverlay}
          />
          <Route
            path="/orgs/:orgID/tasks/import/template"
            component={TaskImportFromTemplateOverlay}
          />
        </Switch>
      </>
    )
  }

  private handleSort = (
    sortKey: TaskSortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ) => {
    this.setState({sortKey, sortDirection, sortType})
  }

  private handleActivate = (task: Task) => {
    this.props.updateTaskStatus(task)
  }

  private handleDelete = (task: Task) => {
    this.props.deleteTask(task.id)
  }

  private handleClone = (task: Task) => {
    this.props.cloneTask(task)
  }

  private handleCreateTask = () => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props

    history.push(`/orgs/${orgID}/tasks/new`)
  }

  private summonImportFromTemplateOverlay = () => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props

    history.push(`/orgs/${orgID}/tasks/import/template`)
  }

  private summonImportOverlay = (): void => {
    const {
      history,
      match: {
        params: {orgID},
      },
    } = this.props

    history.push(`/orgs/${orgID}/tasks/import`)
  }

  private get filteredTasks(): Task[] {
    const {tasks, showInactive} = this.props
    const matchingTasks = tasks.filter(t => {
      let activeFilter = true
      if (!showInactive) {
        activeFilter = t.status === 'active'
      }

      return activeFilter
    })

    return matchingTasks
  }

  private get totalTaskCount(): number {
    return this.props.tasks.length
  }

  private get hiddenTaskAlert(): JSX.Element {
    const {showInactive, tasks} = this.props

    const hiddenCount = tasks.filter(t => t.status === 'inactive').length

    const allTasksAreHidden = hiddenCount === tasks.length

    if (allTasksAreHidden || showInactive) {
      return null
    }

    if (hiddenCount) {
      const pluralizer = hiddenCount === 1 ? '' : 's'
      const verb = hiddenCount === 1 ? 'is' : 'are'

      return (
        <div className="hidden-tasks-alert">{`${hiddenCount} inactive task${pluralizer} ${verb} hidden from view`}</div>
      )
    }
  }
}

const mstp = (state: AppState): ConnectedStateProps => {
  const {
    resources,
    cloud: {limits},
  } = state
  const {status, searchTerm, showInactive} = resources.tasks

  return {
    tasks: getAll<Task>(state, ResourceType.Tasks),
    status: status,
    searchTerm,
    showInactive,
    limitStatus: extractTaskLimits(limits),
  }
}

const mdtp: ConnectedDispatchProps = {
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  cloneTask,
  setSearchTerm: setSearchTermAction,
  setShowInactive: setShowInactiveAction,
  onAddTaskLabel: addTaskLabel,
  onRunTask: runTask,
  checkTaskLimits: checkTasksLimitsAction,
}

export default connect<ConnectedStateProps, ConnectedDispatchProps>(
  mstp,
  mdtp
)(TasksPage)
