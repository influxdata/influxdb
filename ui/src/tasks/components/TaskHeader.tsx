import React, {PureComponent} from 'react'

import {Page} from 'src/pageLayout'
import {ComponentColor, Button, ComponentStatus} from '@influxdata/clockface'

import 'src/tasks/components/TasksPage.scss'

interface Props {
  title: string
  canSubmit: boolean
  onCancel: () => void
  onSave: () => void
}

export default class TaskHeader extends PureComponent<Props> {
  public render() {
    return (
      <Page.Header fullWidth={true}>
        <Page.Header.Left>
          <Page.Title title={this.props.title} />
        </Page.Header.Left>
        <Page.Header.Right>
          <Button
            color={ComponentColor.Default}
            text="Cancel"
            onClick={this.props.onCancel}
          />
          <Button
            color={ComponentColor.Success}
            text="Save"
            status={
              this.props.canSubmit
                ? ComponentStatus.Default
                : ComponentStatus.Disabled
            }
            onClick={this.props.onSave}
          />
        </Page.Header.Right>
      </Page.Header>
    )
  }
}
