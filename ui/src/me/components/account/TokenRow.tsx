// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {
  deleteAuthorization,
  updateAuthorization,
} from 'src/authorizations/actions'

// Components
import {Alignment, ComponentSize, SlideToggle} from '@influxdata/clockface'
import {IndexList, ComponentSpacer, ConfirmationButton} from 'src/clockface'

// Types
import {Authorization} from '@influxdata/influx'

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
    const {description, id} = this.props.auth

    return (
      <IndexList.Row>
        <IndexList.Cell>
          <a
            href="#"
            onClick={this.handleClickDescription}
            data-testid={`token-description-${id}`}
          >
            {description}
          </a>
        </IndexList.Cell>
        <IndexList.Cell>
          <SlideToggle
            active={this.isTokenEnbled}
            size={ComponentSize.ExtraSmall}
            onChange={this.changeToggle}
          />
        </IndexList.Cell>
        <IndexList.Cell alignment={Alignment.Right} revealOnHover={true}>
          <ComponentSpacer align={Alignment.Right}>
            <ConfirmationButton
              size={ComponentSize.ExtraSmall}
              text="Delete"
              confirmText="Confirm"
              onConfirm={this.handleDelete}
            />
          </ComponentSpacer>
        </IndexList.Cell>
      </IndexList.Row>
    )
  }

  private get isTokenEnbled(): boolean {
    const {auth} = this.props
    return auth.status === Authorization.StatusEnum.Active
  }

  private changeToggle = () => {
    const {auth, onUpdate} = this.props
    if (auth.status === Authorization.StatusEnum.Active) {
      auth.status = Authorization.StatusEnum.Inactive
    } else {
      auth.status = Authorization.StatusEnum.Active
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
}

const mdtp = {
  onDelete: deleteAuthorization,
  onUpdate: updateAuthorization,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(TokenRow)
