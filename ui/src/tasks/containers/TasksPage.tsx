// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  populateTasks,
  updateTaskStatus,
  deleteTask,
  selectTask,
  setSearchTerm as setSearchTermAction,
  setShowInactive as setShowInactiveAction,
  setDropdownOrgID as setDropdownOrgIDAction,
} from 'src/tasks/actions/v2'

// Constants
import {allOrganizationsID} from 'src/tasks/constants'

// Types
import {Task, TaskStatus} from 'src/types/v2/tasks'
import {Organization} from 'src/types/v2'

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
}

interface ConnectedStateProps {
  tasks: Task[]
  searchTerm: string
  showInactive: boolean
  orgs: Organization[]
  dropdownOrgID: string
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

@ErrorHandling
class TasksPage extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)

    props.setSearchTerm('')
    if (!props.showInactive) {
      props.setShowInactive()
    }
    props.setDropdownOrgID(null)
  }

  public render(): JSX.Element {
    const {
      setSearchTerm,
      searchTerm,
      setShowInactive,
      showInactive,
    } = this.props
    return (
      <Page>
        <TasksHeader
          onCreateTask={this.handleCreateTask}
          setSearchTerm={setSearchTerm}
          setShowInactive={setShowInactive}
          showInactive={showInactive}
        />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <TasksList
              searchTerm={searchTerm}
              tasks={this.filteredTasks}
              onActivate={this.handleActivate}
              onDelete={this.handleDelete}
              onCreate={this.handleCreateTask}
              onSelect={this.props.selectTask}
            />
          </div>
        </Page.Contents>
      </Page>
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

  private get filteredTasks(): Task[] {
    const {tasks, searchTerm, showInactive, dropdownOrgID} = this.props
    const matchingTasks = tasks.filter(t => {
      const searchTermFilter = t.name
        .toLowerCase()
        .includes(searchTerm.toLowerCase())
      let activeFilter = true
      if (!showInactive) {
        activeFilter = t.status === TaskStatus.Active
      }
      let orgIDFilter = true
      if (dropdownOrgID && dropdownOrgID !== allOrganizationsID) {
        orgIDFilter = t.organizationId === dropdownOrgID
      }
      return searchTermFilter && activeFilter && orgIDFilter
    })

    return matchingTasks
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
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(
  mstp,
  mdtp
)(TasksPage)
