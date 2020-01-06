// Libraries
import React, {PureComponent, MouseEvent} from 'react'
import {connect} from 'react-redux'

// Actions
import {
  deleteAuthorization,
  updateAuthorization,
} from 'src/authorizations/actions/thunks'

// Components
import {
  ComponentSize,
  SlideToggle,
  ComponentColor,
  IndexList,
  Alignment,
  ConfirmationButton,
  Appearance,
} from '@influxdata/clockface'
import EditableName from 'src/shared/components/EditableName'

// Types
import {Authorization} from 'src/types'
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
      <IndexList.Row brighten={true}>
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
            testID="delete-token"
            size={ComponentSize.ExtraSmall}
            text="Delete"
            confirmationLabel="Really delete this token?"
            confirmationButtonText="Confirm"
            confirmationButtonColor={ComponentColor.Danger}
            popoverAppearance={Appearance.Outline}
            popoverColor={ComponentColor.Danger}
            color={ComponentColor.Danger}
            onConfirm={this.handleDelete}
          />
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private get isTokenEnabled(): boolean {
    const {auth} = this.props
    return auth.status === 'active'
  }

  private changeToggle = () => {
    const {auth, onUpdate} = this.props
    if (auth.status === 'active') {
      auth.status = 'inactive'
    } else {
      auth.status = 'active'
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

  private handleUpdateName = (value: string) => {
    const {auth, onUpdate} = this.props
    onUpdate({...auth, description: value})
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
