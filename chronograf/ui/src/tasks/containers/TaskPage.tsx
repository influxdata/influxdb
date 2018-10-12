import _ from 'lodash'
import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'
import {connect} from 'react-redux'
import FluxEditor from 'src/flux/components/v2/FluxEditor'

import {Links} from 'src/types/v2/links'
import {State as TasksState} from 'src/tasks/reducers/v2'
import {setNewScript, saveNewScript} from 'src/tasks/actions/v2'
import {
  Form,
  Columns,
  ComponentColor,
  ComponentSize,
  Button,
} from 'src/clockface'
import {Page} from 'src/pageLayout'

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
        <Page.Header fullWidth={true}>
          <Page.Header.Left>
            <Page.Title title="Create Task" />
          </Page.Header.Left>
          <Page.Header.Right>
            <Button
              color={ComponentColor.Default}
              text="Cancel"
              onClick={this.handleCancel}
              size={ComponentSize.Medium}
            />
            <Button
              color={ComponentColor.Success}
              text="Save"
              onClick={this.handleSave}
              size={ComponentSize.Medium}
            />
          </Page.Header.Right>
        </Page.Header>
        <Form style={{height: '90%'}}>
          <Form.Element
            label="Script"
            colsXS={Columns.Six}
            offsetXS={Columns.Three}
            errorMessage={''}
          >
            <FluxEditor
              script={newScript}
              onChangeScript={this.handleChange}
              visibility={'visible'}
              status={{text: '', type: ''}}
              onSubmitScript={_.noop}
              suggestions={[]}
            />
          </Form.Element>
        </Form>
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
    this.props.router.push('/tasks')
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
}

export default connect<ConnectStateProps, ConnectDispatchProps, {}>(mstp, mdtp)(
  TaskPage
)
