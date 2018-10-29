import _ from 'lodash'
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'

import TaskForm from 'src/tasks/components/TaskForm'
import TaskHeader from 'src/tasks/components/TaskHeader'

import {Links} from 'src/types/v2/links'
import {State as TasksState} from 'src/tasks/reducers/v2'
import {setNewScript, saveNewScript, goToTasks} from 'src/tasks/actions/v2'

interface PassedInProps {
  router: InjectedRouter
}

interface ConnectStateProps {
  newScript: string
  tasksLink: string
}

interface ConnectDispatchProps {
  setNewScript: typeof setNewScript
  saveNewScript: typeof saveNewScript
  goToTasks: typeof goToTasks
}

class TaskPage extends PureComponent<
  PassedInProps & ConnectStateProps & ConnectDispatchProps
> {
  constructor(props) {
    super(props)
  }

  public render(): JSX.Element {
    const {newScript} = this.props

    return (
      <div className="page">
        <TaskHeader
          title="Create Task"
          onCancel={this.handleCancel}
          onSave={this.handleSave}
        />
        <TaskForm script={newScript} onChange={this.handleChange} />
      </div>
    )
  }

  private handleChange = (script: string) => {
    this.props.setNewScript(script)
  }

  private handleSave = () => {
    this.props.saveNewScript()
  }

  private handleCancel = () => {
    this.props.goToTasks()
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
    newScript: tasks.newScript,
    tasksLink: links.tasks,
  }
}

const mdtp: ConnectDispatchProps = {
  setNewScript,
  saveNewScript,
  goToTasks,
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(mstp, mdtp)(
  TaskPage
)
