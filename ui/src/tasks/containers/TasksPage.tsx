// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {OverlayTechnology} from 'src/clockface'
import ImportTaskOverlay from 'src/tasks/components/ImportTaskOverlay'

// Actions
import {
  populateTasks,
  updateTaskStatus,
  deleteTask,
  selectTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  setDropdownOrgID as setDropdownOrgIDAction,
  importScript,
} from 'src/tasks/actions/v2'

// Constants
import {allOrganizationsID} from 'src/tasks/constants'

// Types
import {Task as TaskAPI, User, Organization} from 'src/api'

interface Task extends TaskAPI {
  organization: Organization
  owner?: User
  delay?: string
}

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  populateTasks: typeof populateTasks
  updateTaskStatus: typeof updateTaskStatus
  deleteTask: typeof deleteTask
  selectTask: typeof selectTask
  setSearchTerm: typeof setSearchTermAction
  setShowInactive: typeof setShowInactiveAction
  setDropdownOrgID: typeof setDropdownOrgIDAction
  importScript: typeof importScript
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
  isOverlayVisible: boolean
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

    this.state = {isOverlayVisible: false}
  }

  public render(): JSX.Element {
    const {
      setSearchTerm,
      searchTerm,
      setShowInactive,
      showInactive,
    } = this.props

    return (
      <>
        <Page>
          <TasksHeader
            onCreateTask={this.handleCreateTask}
            setSearchTerm={setSearchTerm}
            setShowInactive={setShowInactive}
            showInactive={showInactive}
            toggleOverlay={this.handleToggleOverlay}
          />
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <TasksList
                searchTerm={searchTerm}
                tasks={this.filteredTasks}
                totalCount={this.totalTaskCount}
                onActivate={this.handleActivate}
                onDelete={this.handleDelete}
                onCreate={this.handleCreateTask}
                onSelect={this.props.selectTask}
              />
              {this.hiddenTaskAlert}
            </div>
          </Page.Contents>
        </Page>
        {this.renderImportOverlay}
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

  private handleCreateTask = () => {
    const {router} = this.props

    router.push('/tasks/new')
  }

  private handleToggleOverlay = () => {
    this.setState({isOverlayVisible: !this.state.isOverlayVisible})
  }

  private handleSave = (script: string, fileName: string) => {
    this.props.importScript(script, fileName)
  }

  private get renderImportOverlay(): JSX.Element {
    const {isOverlayVisible} = this.state

    return (
      <OverlayTechnology visible={isOverlayVisible}>
        <ImportTaskOverlay
          onDismissOverlay={this.handleToggleOverlay}
          onSave={this.handleSave}
        />
      </OverlayTechnology>
    )
  }

  private get filteredTasks(): Task[] {
    const {tasks, searchTerm, showInactive, dropdownOrgID} = this.props
    const matchingTasks = tasks.filter(t => {
      const searchTermFilter = t.name
        .toLowerCase()
        .includes(searchTerm.toLowerCase())
      let activeFilter = true
      if (!showInactive) {
        activeFilter = t.status === TaskAPI.StatusEnum.Active
      }
      let orgIDFilter = true
      if (dropdownOrgID && dropdownOrgID !== allOrganizationsID) {
        orgIDFilter = t.organizationId === dropdownOrgID
      }
      return searchTermFilter && activeFilter && orgIDFilter
    })

    return matchingTasks
  }

  private get totalTaskCount(): number {
    return this.props.tasks.length
  }

  private get hiddenTaskAlert(): JSX.Element {
    const {showInactive, tasks} = this.props

    const hiddenCount = tasks.filter(
      t => t.status === TaskAPI.StatusEnum.Inactive
    ).length

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
}): ConnectedStateProps => {
  return {
    tasks,
    searchTerm,
    showInactive,
    orgs,
    dropdownOrgID,
  }
}

const mdtp: ConnectedDispatchProps = {
  populateTasks,
  updateTaskStatus,
  deleteTask,
  selectTask,
  setSearchTerm: setSearchTermAction,
  setShowInactive: setShowInactiveAction,
  setDropdownOrgID: setDropdownOrgIDAction,
  importScript,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(
  mstp,
  mdtp
)(TasksPage)
