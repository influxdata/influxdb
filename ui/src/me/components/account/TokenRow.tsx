// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Actions
import {deleteAuthorization} from 'src/authorizations/actions'

// Components
import {
  Alignment,
  Button,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'
import {IndexList, ComponentSpacer} from 'src/clockface'

// Types
import {Authorization} from '@influxdata/influx'

interface OwnProps {
  auth: Authorization
  onClickDescription: (authID: string) => void
}

interface DispatchProps {
  onDelete: typeof deleteAuthorization
}

type Props = DispatchProps & OwnProps

class TokenRow extends PureComponent<Props> {
  public render() {
    const {description, status, id} = this.props.auth

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
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(TokenRow)
