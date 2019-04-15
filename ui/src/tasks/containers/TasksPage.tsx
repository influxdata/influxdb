// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FilterList from 'src/shared/components/Filter'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import GetLabels from 'src/labels/components/GetLabels'
import GetResources, {ResourceTypes} from 'src/shared/components/GetResources'

// Actions
import {
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  addTaskLabelsAsync,
  removeTaskLabelsAsync,
  runTask,
} from 'src/tasks/actions'

// Types
import {AppState, Task, TaskStatus, RemoteDataState} from 'src/types'
import {InjectedRouter, WithRouterProps} from 'react-router'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  updateTaskStatus: typeof updateTaskStatus
  updateTaskName: typeof updateTaskName
  deleteTask: typeof deleteTask
  cloneTask: typeof cloneTask
  selectTask: typeof selectTask
  setSearchTerm: typeof setSearchTermAction
  setShowInactive: typeof setShowInactiveAction
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
  onRunTask: typeof runTask
}

interface ConnectedStateProps {
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  status: RemoteDataState
}

type Props = ConnectedDispatchProps &
  PassedInProps &
  ConnectedStateProps &
  WithRouterProps

interface State {
  isImporting: boolean
  taskLabelsEdit: Task
}

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
    }
  }

  public render(): JSX.Element {
    const {
      setSearchTerm,
      updateTaskName,
      searchTerm,
      setShowInactive,
      showInactive,
      onAddTaskLabels,
      onRemoveTaskLabels,
      onRunTask,
    } = this.props

    return (
      <>
        <Page titleTag="Tasks">
          <TasksHeader
            onCreateTask={this.handleCreateTask}
            setShowInactive={setShowInactive}
            showInactive={showInactive}
            filterComponent={() => this.search}
            onImportTask={this.summonOverlay}
          />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <GetResources resource={ResourceTypes.Tasks}>
                <GetLabels>
                  <FilterList<Task>
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
                        onSelect={this.props.selectTask}
                        onAddTaskLabels={onAddTaskLabels}
                        onRemoveTaskLabels={onRemoveTaskLabels}
                        onRunTask={onRunTask}
                        onFilterChange={setSearchTerm}
                        filterComponent={() => this.search}
                        onUpdate={updateTaskName}
                        onImportTask={this.summonOverlay}
                      />
                    )}
                  </FilterList>
                  {this.hiddenTaskAlert}
                </GetLabels>
              </GetResources>
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  private handleActivate = (task: Task) => {
    this.props.updateTaskStatus(task)
  }

  private handleDelete = (task: Task) => {
    this.props.deleteTask(task)
  }

  private handleClone = (task: Task) => {
    const {tasks} = this.props
    this.props.cloneTask(task, tasks)
  }

  private handleCreateTask = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tasks/new`)
  }

  private summonOverlay = (): void => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/tasks/import`)
  }

  private get search(): JSX.Element {
    const {setSearchTerm, searchTerm} = this.props

    return (
      <SearchWidget
        placeholderText="Filter tasks..."
        onSearch={setSearchTerm}
        searchTerm={searchTerm}
      />
    )
  }

  private get filteredTasks(): Task[] {
    const {tasks, showInactive} = this.props
    const matchingTasks = tasks.filter(t => {
      let activeFilter = true
      if (!showInactive) {
        activeFilter = t.status === TaskStatus.Active
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

    const hiddenCount = tasks.filter(t => t.status === TaskStatus.Inactive)
      .length

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

const mstp = ({
  tasks: {status, list, searchTerm, showInactive},
}: AppState): ConnectedStateProps => {
  return {
    tasks: list,
    status: status,
    searchTerm,
    showInactive,
  }
}

const mdtp: ConnectedDispatchProps = {
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm: setSearchTermAction,
  setShowInactive: setShowInactiveAction,
  onRemoveTaskLabels: removeTaskLabelsAsync,
  onAddTaskLabels: addTaskLabelsAsync,
  onRunTask: runTask,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(
  mstp,
  mdtp
)(TasksPage)
