// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'

// Actions
import {
  populateTasks,
  deleteTask,
  setSearchTerm as setSearchTermAction,
} from 'src/tasks/actions/v2'

// Types
import {Task} from 'src/types/v2/tasks'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  populateTasks: typeof populateTasks
  deleteTask: typeof deleteTask
  setSearchTerm: typeof setSearchTermAction
}

interface ConnectedStateProps {
  tasks: Task[]
  searchTerm: string
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

@ErrorHandling
class TasksPage extends PureComponent<Props> {
  constructor(props) {
    super(props)

    this.props.setSearchTerm('')
  }

  public render(): JSX.Element {
    const {setSearchTerm} = this.props

    return (
      <Page>
        <TasksHeader
          onCreateTask={this.handleCreateTask}
          filterTasks={setSearchTerm}
        />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <TasksList
              tasks={this.filteredTasks}
              onDelete={this.handleDelete}
              onCreate={this.handleCreateTask}
            />
          </div>
        </Page.Contents>
      </Page>
    )
  }

  public componentDidMount() {
    this.props.populateTasks()
  }

  private handleDelete = (task: Task) => {
    this.props.deleteTask(task)
  }

  private handleCreateTask = () => {
    const {router} = this.props

    router.push('/tasks/new')
  }

  private get filteredTasks(): Task[] {
    const {tasks, searchTerm} = this.props

    const matchingTasks = tasks.filter(t =>
      t.name.toLowerCase().includes(searchTerm.toLowerCase())
    )

    return matchingTasks
  }
}

const mstp = ({tasks: {tasks, searchTerm}}): ConnectedStateProps => {
  return {tasks, searchTerm}
}

const mdtp: ConnectedDispatchProps = {
  populateTasks,
  deleteTask,
  setSearchTerm: setSearchTermAction,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(mstp, mdtp)(TasksPage)
