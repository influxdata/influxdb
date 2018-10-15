// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

// Components
import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'
import {Page} from 'src/pageLayout'

// Actions
import {populateTasks, deleteTask} from 'src/tasks/actions/v2'

// Types
import {Task} from 'src/types/v2/tasks'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  populateTasks: typeof populateTasks
  deleteTask: typeof deleteTask
}

interface ConnectedStateProps {
  tasks: Task[]
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

@ErrorHandling
class TasksPage extends PureComponent<Props> {
  public render(): JSX.Element {
    const {tasks} = this.props

    return (
      <Page>
        <TasksHeader onCreateTask={this.handleCreateTask} />
        <Page.Contents fullWidth={false} scrollable={true}>
          <div className="col-xs-12">
            <TasksList
              tasks={tasks}
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
}

const mstp = ({tasks: {tasks}}): ConnectedStateProps => {
  return {tasks}
}

const mdtp: ConnectedDispatchProps = {
  populateTasks,
  deleteTask,
}

export default connect<
  ConnectedStateProps,
  ConnectedDispatchProps,
  PassedInProps
>(mstp, mdtp)(TasksPage)
