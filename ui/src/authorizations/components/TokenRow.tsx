// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'

// Actions
import {
  deleteAuthorization,
  updateAuthorization,
} from 'src/authorizations/actions'

// Components
import {ComponentSize, SlideToggle, ComponentColor} from '@influxdata/clockface'
import {IndexList, ConfirmationButton, Alignment} from 'src/clockface'
import EditableName from 'src/shared/components/EditableName'

// Types
import {Authorization, AuthorizationUpdateRequest} from '@influxdata/influx'
import {DEFAULT_TOKEN_DESCRIPTION} from 'src/dashboards/constants'

interface OwnProps {
  auth: Authorization
  onClickDescription: (authID: string) => void
}

interface DispatchProps {
  onDelete: typeof deleteAuthorization
  onUpdate: typeof updateAuthorization
}

type Props = DispatchProps & OwnProps

class TokenRow extends PureComponent<Props> {
  public render() {
    const {description} = this.props.auth

    return (
      <IndexList.Row>
        <IndexList.Cell>
          <EditableName
            name={description}
            onUpdate={this.handleUpdateName}
            noNameString={DEFAULT_TOKEN_DESCRIPTION}
            onEditName={this.handleClickDescription}
          />
        </IndexList.Cell>
        <IndexList.Cell>
          <SlideToggle
            active={this.isTokenEnabled}
            size={ComponentSize.ExtraSmall}
            onChange={this.changeToggle}
            color={ComponentColor.Success}
          />
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ConfirmationButton
            size={ComponentSize.ExtraSmall}
            text="Delete"
            confirmText="Confirm"
            onConfirm={this.handleDelete}
          />
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private get isTokenEnabled(): boolean {
    const {auth} = this.props
    return auth.status === AuthorizationUpdateRequest.StatusEnum.Active
  }

  private changeToggle = () => {
    const {auth, onUpdate} = this.props
    if (auth.status === AuthorizationUpdateRequest.StatusEnum.Active) {
      auth.status = AuthorizationUpdateRequest.StatusEnum.Inactive
    } else {
      auth.status = AuthorizationUpdateRequest.StatusEnum.Active
    }
    onUpdate(auth)
  }

  private handleDelete = () => {
    const {id, description} = this.props.auth
    this.props.onDelete(id, description)
  }

  private handleClickDescription = (e: MouseEvent<HTMLAnchorElement>) => {
    const {onClickDescription, auth} = this.props

    e.preventDefault()

    onClickDescription(auth.id)
  }

  private handleUpdateName = async (value: string) => {
    const {auth, onUpdate} = this.props
    await onUpdate({...auth, description: value})
  }
}

const mdtp = {
  onDelete: deleteAuthorization,
  onUpdate: updateAuthorization,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(TokenRow)
