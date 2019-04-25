// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {
  deleteAuthorization,
  updateAuthorization,
} from 'src/authorizations/actions'

// Components
import {ComponentSize, SlideToggle} from '@influxdata/clockface'
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
            onUpdate={this.handleUpdateName}
            name={description}
            noNameString={DEFAULT_TOKEN_DESCRIPTION}
            onEditName={this.handleClickDescription}
          />
        </IndexList.Cell>
        <IndexList.Cell>
          <SlideToggle
            active={this.isTokenEnbled}
            size={ComponentSize.ExtraSmall}
            onChange={this.changeToggle}
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

  private get isTokenEnbled(): boolean {
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

  private handleClickDescription = () => {
    const {onClickDescription, auth} = this.props
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
