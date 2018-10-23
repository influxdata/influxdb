import React, {PureComponent} from 'react'

import {Page} from 'src/pageLayout'
import {ComponentColor, ComponentSize, Button} from 'src/clockface'

interface Props {
  title: string
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
            size={ComponentSize.Medium}
          />
          <Button
            color={ComponentColor.Success}
            text="Save"
            onClick={this.props.onSave}
            size={ComponentSize.Medium}
          />
        </Page.Header.Right>
      </Page.Header>
    )
  }
}
