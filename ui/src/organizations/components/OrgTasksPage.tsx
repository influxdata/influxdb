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
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'

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
import {Organization, ITask as Task} from '@influxdata/influx'
import {AppState, TaskStatus} from 'src/types/v2'
import {client} from 'src/utils/api'
import GetLabels from 'src/configuration/components/GetLabels'

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
          setShowInactive={this.handleToggle}
          showInactive={showInactive}
          onImportTask={this.handleImportTask}
          showOrgDropdown={false}
          isFullPage={false}
          filterComponent={() => this.filterComponent}
        />
        <GetLabels>
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
                onFilterChange={setSearchTerm}
                onImportTask={this.handleImportTask}
              />
            )}
          </FilterList>
        </GetLabels>
      </>
    )
  }

  private get filterComponent(): JSX.Element {
    const {setSearchTerm, searchTerm} = this.props

    return (
      <SearchWidget
        placeholderText="Filter tasks..."
        onSearch={setSearchTerm}
        searchTerm={searchTerm}
      />
    )
  }

  private handleUpdateTask = async (task: Task) => {
    await client.tasks.update(task.id, task)
    this.props.onChange()
  }

  private handleSelectTask = (task: Task) => {
    const {selectTask, orgID} = this.props

    selectTask(task, `/organizations/${orgID}/tasks/${task.id}`)
  }

  private get filteredTasks() {
    const {tasks, showInactive} = this.props
    if (showInactive) {
      return tasks
    }
    const mappedTasks = tasks.filter(t => {
      if (!showInactive) {
        return t.status === TaskStatus.Active
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

    router.push(`/organizations/${orgID}/tasks/new`)
  }

  private handleImportTask = (): void => {
    const {router, orgID} = this.props

    router.push(`/organizations/${orgID}/tasks/import`)
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
