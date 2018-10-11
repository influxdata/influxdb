import React, {PureComponent} from 'react'
import {InjectedRouter} from 'react-router'

import {Page} from 'src/pageLayout'
import {Button, ComponentColor, IconFont} from 'src/clockface'

interface Props {
  router: InjectedRouter
}

class TasksPage extends PureComponent<Props> {
  public render(): JSX.Element {
    return (
      <div className="page">
        <Page.Header fullWidth={true}>
          <Page.Header.Left>
            <Page.Title title="Tasks" />
          </Page.Header.Left>
          <Page.Header.Right>
            <Button
              color={ComponentColor.Primary}
              onClick={this.handleCreateTask}
              icon={IconFont.Plus}
              text="Create Task"
              titleText="Create a new task"
            />
          </Page.Header.Right>
        </Page.Header>
        YO!
      </div>
    )
  }

  private handleCreateTask = () => {
    const {router} = this.props
    router.push('/tasks/new')
  }
}

export default TasksPage
