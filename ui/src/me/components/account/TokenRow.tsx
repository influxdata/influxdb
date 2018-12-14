// Libraries
import React, {PureComponent} from 'react'

// Components
import {
  IndexList,
  ComponentSpacer,
  Alignment,
  Button,
  ComponentSize,
  ComponentColor,
} from 'src/clockface'

// Types
import {Authorization} from 'src/api'

// Actions
import {notify} from 'src/shared/actions/notifications'

interface Props {
  auth: Authorization
  onNotify: typeof notify
  onDelete: (authID: string) => void
}

export default class TokenRow extends PureComponent<Props> {
  public render() {
    const {description, status} = this.props.auth

    return (
      <IndexList.Row>
        <IndexList.Cell>{description}</IndexList.Cell>
        <IndexList.Cell>{status}</IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <Button
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Danger}
              text="Delete"
              onClick={this.handleDelete}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private handleDelete = () => {
    const {
      auth: {id},
      onDelete,
    } = this.props
    onDelete(id)
  }
}
