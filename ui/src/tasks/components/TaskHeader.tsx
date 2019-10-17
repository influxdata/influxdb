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
    return (
      <Page.Header fullWidth={true}>
        <Page.HeaderLeft>
          <PageTitleWithOrg title={this.props.title} />
        </Page.HeaderLeft>
        <Page.HeaderRight>
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
        </Page.HeaderRight>
      </Page.Header>
    )
  }
}
