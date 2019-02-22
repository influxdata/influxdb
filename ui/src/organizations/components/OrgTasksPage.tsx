// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'
import _ from 'lodash'

// Components
import FilterList from 'src/shared/components/Filter'
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {ErrorHandling} from 'src/shared/decorators/errors'
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {
  updateTaskStatus,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  importTask,
  addTaskLabelsAsync,
  removeTaskLabelsAsync,
  runTask,
} from 'src/tasks/actions/v2'

// Types
import {Task as TaskAPI, Organization} from '@influxdata/influx'
import {Task} from 'src/tasks/containers/TasksPage'
import {AppState} from 'src/types/v2'
import {client} from 'src/utils/api'

interface PassedInProps {
  tasks: Task[]
  orgName: string
  orgID: string
  onChange: () => void
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  updateTaskStatus: typeof updateTaskStatus
  deleteTask: typeof deleteTask
  cloneTask: typeof cloneTask
  selectTask: typeof selectTask
  setSearchTerm: typeof setSearchTermAction
  setShowInactive: typeof setShowInactiveAction
  importTask: typeof importTask
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
  onRunTask: typeof runTask
}

interface ConnectedStateProps {
  searchTerm: string
  showInactive: boolean
  orgs: Organization[]
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

interface State {
  isImporting: boolean
  taskLabelsEdit: Task
}

@ErrorHandling
class OrgTasksPage extends PureComponent<Props, State> {
  constructor(props) {
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

  public render() {
    const {
      setSearchTerm,
      showInactive,
      searchTerm,
      onAddTaskLabels,
      onRemoveTaskLabels,
      onRunTask,
    } = this.props

    return (
      <>
        <TasksHeader
          onCreateTask={this.handleCreateTask}
          setSearchTerm={setSearchTerm}
          setShowInactive={this.handleToggle}
          showInactive={showInactive}
          toggleOverlay={this.handleToggleImportOverlay}
          showOrgDropdown={false}
          isFullPage={false}
        />
        <FilterList<Task>
          searchTerm={searchTerm}
          searchKeys={['name', 'labels[].name']}
          list={this.filteredTasks}
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
              onSelect={this.handleSelectTask}
              onAddTaskLabels={onAddTaskLabels}
              onRemoveTaskLabels={onRemoveTaskLabels}
              onUpdate={this.handleUpdateTask}
              onRunTask={onRunTask}
            />
          )}
        </FilterList>
        {this.importOverlay}
      </>
    )
  }

  private handleUpdateTask = async (task: Task) => {
    await client.tasks.update(task.id, task)
    this.props.onChange()
  }

  private handleSelectTask = (task: Task) => {
    const {selectTask, orgID} = this.props

    selectTask(task, `organizations/${orgID}/tasks_tab/${task.id}`)
  }

  private get filteredTasks() {
    const {tasks, showInactive} = this.props
    if (showInactive) {
      return tasks
    }
    const mappedTasks = tasks.filter(t => {
      if (!showInactive) {
        return t.status === TaskAPI.StatusEnum.Active
      }
    })

    return mappedTasks
  }

  private handleToggle = async () => {
    await this.props.setShowInactive()
    this.props.onChange()
  }

  private get totalTaskCount(): number {
    return this.props.tasks.length
  }

  private handleActivate = async (task: Task) => {
    await this.props.updateTaskStatus(task)
    this.props.onChange()
  }

  private handleDelete = async (task: Task) => {
    await this.props.deleteTask(task)
    this.props.onChange()
  }

  private handleClone = async (task: Task) => {
    const {tasks} = this.props
    await this.props.cloneTask(task, tasks)
    this.props.onChange()
  }

  private handleCreateTask = () => {
    const {router, orgID} = this.props

    router.push(`/organizations/${orgID}/tasks_tab/new`)
  }

  private handleToggleImportOverlay = (): void => {
    this.setState({isImporting: !this.state.isImporting})
  }

  private get importOverlay(): JSX.Element {
    const {isImporting} = this.state
    const {importTask} = this.props
    return (
      <ImportOverlay
        isVisible={isImporting}
        resourceName="Task"
        onDismissOverlay={this.handleToggleImportOverlay}
        onImport={importTask}
        isResourceValid={this.handleValidateTask}
      />
    )
  }

  private handleValidateTask = (): boolean => {
    return true
  }
}

const mstp = ({
  tasks: {searchTerm, showInactive},
  orgs,
}: AppState): ConnectedStateProps => {
  return {
    searchTerm,
    showInactive,
    orgs,
  }
}

const mdtp: ConnectedDispatchProps = {
  updateTaskStatus,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm: setSearchTermAction,
  setShowInactive: setShowInactiveAction,
  importTask,
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
)(OrgTasksPage)
