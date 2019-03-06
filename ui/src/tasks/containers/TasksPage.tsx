// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'
import _ from 'lodash'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'
import FilterList from 'src/shared/components/Filter'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'

// Actions
import {
  populateTasks,
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  setDropdownOrgID as setDropdownOrgIDAction,
  importTask,
  addTaskLabelsAsync,
  removeTaskLabelsAsync,
  runTask,
} from 'src/tasks/actions/v2'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {allOrganizationsID} from 'src/tasks/constants'

// Types
import {Organization} from '@influxdata/influx'
import {AppState, Task, TaskStatus} from 'src/types/v2'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  populateTasks: typeof populateTasks
  updateTaskStatus: typeof updateTaskStatus
  updateTaskName: typeof updateTaskName
  deleteTask: typeof deleteTask
  cloneTask: typeof cloneTask
  selectTask: typeof selectTask
  setSearchTerm: typeof setSearchTermAction
  setShowInactive: typeof setShowInactiveAction
  setDropdownOrgID: typeof setDropdownOrgIDAction
  importTask: typeof importTask
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
  onRunTask: typeof runTask
  notify: typeof notifyAction
}

interface ConnectedStateProps {
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  orgs: Organization[]
  dropdownOrgID: string
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

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
    props.setDropdownOrgID(null)

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
                  />
                )}
              </FilterList>
              {this.hiddenTaskAlert}
            </div>
          </Page.Contents>
        </Page>
        {this.props.children}
      </>
    )
  }

  public componentDidMount() {
    this.props.populateTasks()
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
    const {router} = this.props

    router.push('/tasks/new')
  }

  private summonOverlay = (): void => {
    const {router} = this.props

    router.push('/tasks/import')
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
    const {tasks, showInactive, dropdownOrgID} = this.props
    const matchingTasks = tasks.filter(t => {
      let activeFilter = true
      if (!showInactive) {
        activeFilter = t.status === TaskStatus.Active
      }
      let orgIDFilter = true
      if (dropdownOrgID && dropdownOrgID !== allOrganizationsID) {
        orgIDFilter = t.orgID === dropdownOrgID
      }
      return activeFilter && orgIDFilter
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
  tasks: {tasks, searchTerm, showInactive, dropdownOrgID},
  orgs,
}: AppState): ConnectedStateProps => {
  return {
    tasks,
    searchTerm,
    showInactive,
    orgs,
    dropdownOrgID,
  }
}

const mdtp: ConnectedDispatchProps = {
  notify: notifyAction,
  populateTasks,
  updateTaskStatus,
  updateTaskName,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm: setSearchTermAction,
  setShowInactive: setShowInactiveAction,
  setDropdownOrgID: setDropdownOrgIDAction,
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
)(TasksPage)
