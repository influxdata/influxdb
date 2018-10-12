import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {InjectedRouter} from 'react-router'

import TasksHeader from 'src/tasks/components/TasksHeader'
import TasksList from 'src/tasks/components/TasksList'

import {populateTasks} from 'src/tasks/actions/v2'
import {Task} from 'src/types/v2/tasks'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectedDispatchProps {
  populateTasks: typeof populateTasks
}

interface ConnectedStateProps {
  tasks: Task[]
}

type Props = ConnectedDispatchProps & PassedInProps & ConnectedStateProps

class TasksPage extends PureComponent<Props> {
  public render(): JSX.Element {
    const {tasks} = this.props

    return (
      <div className="page">
        <TasksHeader onCreateTask={this.handleCreateTask} />
        <TasksList tasks={tasks} />
      </div>
    )
  }

  public componentDidMount() {
    this.props.populateTasks()
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
}

export default connect(mstp, mdtp)(TasksPage) as React.ComponentClass<
  PassedInProps
>
