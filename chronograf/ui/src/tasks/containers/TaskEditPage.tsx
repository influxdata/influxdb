import _ from 'lodash'
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'
import {Task} from 'src/types/v2/tasks'

import {Links} from 'src/types/v2/links'
import {State as TasksState} from 'src/tasks/reducers/v2'
import {
  updateScript,
  selectTaskByID,
  setCurrentScript,
  cancelUpdateTask,
} from 'src/tasks/actions/v2'

interface PassedInProps {
  router: InjectedRouter
  params: {id: string}
}

interface ConnectStateProps {
  currentTask: Task
  currentScript: string
  tasksLink: string
}

interface ConnectDispatchProps {
  setCurrentScript: typeof setCurrentScript
  updateScript: typeof updateScript
  cancelUpdateTask: typeof cancelUpdateTask
  selectTaskByID: typeof selectTaskByID
}

class TaskPage extends PureComponent<
  PassedInProps & ConnectStateProps & ConnectDispatchProps
> {
  constructor(props) {
    super(props)
  }

  public componentDidMount() {
    this.props.selectTaskByID(this.props.params.id)
  }

  public render(): JSX.Element {
    const {currentScript} = this.props

    return (
      <div className="page">
        <TaskHeader
          title="Update Task"
          onCancel={this.handleCancel}
          onSave={this.handleSave}
        />
        <TaskForm script={currentScript} onChange={this.handleChange} />
      </div>
    )
  }

  private handleChange = (script: string) => {
    this.props.setCurrentScript(script)
  }

  private handleSave = () => {
    this.props.updateScript()
  }

  private handleCancel = () => {
    this.props.cancelUpdateTask()
  }
}

const mstp = ({
  tasks,
  links,
}: {
  tasks: TasksState
  links: Links
}): ConnectStateProps => {
  return {
    currentScript: tasks.currentScript,
    tasksLink: links.tasks,
    currentTask: tasks.currentTask,
  }
}

const mdtp: ConnectDispatchProps = {
  setCurrentScript,
  updateScript,
  cancelUpdateTask,
  selectTaskByID,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(mstp, mdtp)(
  TaskPage
)
