import React, {PureComponent} from 'react'
import {Page} from 'src/pageLayout'
import {Button, ComponentColor, IconFont} from 'src/clockface'

interface Props {
  onCreateTask: () => void
}

export default class TasksHeader extends PureComponent<Props> {
  public render() {
    const {onCreateTask} = this.props

    return (
      <Page.Header fullWidth={false}>
        <Page.Header.Left>
          <Page.Title title="Tasks" />
        </Page.Header.Left>
        <Page.Header.Right>
          <Button
            color={ComponentColor.Primary}
            onClick={onCreateTask}
            icon={IconFont.Plus}
            text="Create Task"
            titleText="Create a new Task"
          />
        </Page.Header.Right>
      </Page.Header>
    )
  }
}
