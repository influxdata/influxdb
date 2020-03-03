// Libraries
import React, {PureComponent} from 'react'

// Components
import PageTitleWithOrg from 'src/shared/components/PageTitleWithOrg'

// Types
import {
  ComponentColor,
  Button,
  ComponentStatus,
  Page,
} from '@influxdata/clockface'

interface Props {
  title: string
  canSubmit: boolean
  onCancel: () => void
  onSave: () => void
}

export default class TaskHeader extends PureComponent<Props> {
  public render() {
    const {onCancel, onSave, title} = this.props
    return (
      <Page.ControlBar fullWidth={true}>
        <Page.ControlBarLeft>
          <PageTitleWithOrg title={title} />
        </Page.ControlBarLeft>
        <Page.ControlBarRight>
          <Button
            color={ComponentColor.Default}
            text="Cancel"
            onClick={onCancel}
            testID="task-cancel-btn"
          />
          <Button
            color={ComponentColor.Success}
            text="Save"
            status={this.status}
            onClick={onSave}
            testID="task-save-btn"
          />
        </Page.ControlBarRight>
      </Page.ControlBar>
    )
  }

  private get status() {
    return this.props.canSubmit
      ? ComponentStatus.Default
      : ComponentStatus.Disabled
  }
}
