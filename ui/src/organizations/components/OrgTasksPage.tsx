// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'
import _ from 'lodash'

// Components
import {Input, IconFont} from 'src/clockface'
import FilterList from 'src/shared/components/Filter'
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OverlayTechnology} from 'src/clockface'
import ImportTaskOverlay from 'src/tasks/components/ImportTaskOverlay'

// Actions
import {
  updateTaskStatus,
  deleteTask,
  selectTask,
  cloneTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  importScript,
  addTaskLabelsAsync,
  removeTaskLabelsAsync,
} from 'src/tasks/actions/v2'

// Types
import {Task as TaskAPI, Organization} from '@influxdata/influx'
import {Task} from 'src/tasks/containers/TasksPage'
import {AppState} from 'src/types/v2'

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
  importScript: typeof importScript
  onAddTaskLabels: typeof addTaskLabelsAsync
  onRemoveTaskLabels: typeof removeTaskLabelsAsync
}

interface ConnectedStateProps {
  searchTerm: string
  showInactive: boolean
  orgs: Organization[]
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

interface State {
  isImportOverlayVisible: boolean
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
      isImportOverlayVisible: false,
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
    } = this.props

    return (
      <>
        <Page titleTag="Tasks">
          <Input
            icon={IconFont.Search}
            placeholder="Filter tasks..."
            widthPixels={290}
            value={searchTerm}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
          <TasksHeader
            onCreateTask={this.handleCreateTask}
            setSearchTerm={setSearchTerm}
            setShowInactive={this.handleToggle}
            showInactive={showInactive}
            toggleOverlay={this.handleToggleOverlay}
            showOrgDropdown={false}
            showFilter={false}
          />
          <FilterList<Task>
            searchTerm={searchTerm}
            searchKeys={['name']}
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
              />
            )}
          </FilterList>
        </Page>
        {this.renderImportOverlay}
      </>
    )
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

  private handleToggleOverlay = () => {
    this.setState({isImportOverlayVisible: !this.state.isImportOverlayVisible})
  }

  private handleSave = (script: string, fileName: string) => {
    this.props.importScript(script, fileName)
  }

  private get renderImportOverlay(): JSX.Element {
    const {isImportOverlayVisible} = this.state

    return (
      <OverlayTechnology visible={isImportOverlayVisible}>
        <ImportTaskOverlay
          onDismissOverlay={this.handleToggleOverlay}
          onSave={this.handleSave}
        />
      </OverlayTechnology>
    )
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.props.setSearchTerm(e.target.value)
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.props.setSearchTerm(e.target.value)
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
  importScript,
  onRemoveTaskLabels: removeTaskLabelsAsync,
  onAddTaskLabels: addTaskLabelsAsync,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(
  mstp,
  mdtp
)(OrgTasksPage)
