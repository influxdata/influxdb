// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'

// Types
import {Authorization} from 'src/api'

interface Props {
  auth: Authorization
}

export default class TokenRow extends PureComponent<Props> {
  public render() {
    const {description, status} = this.props.auth

    return (
      <IndexList.Row>
        <IndexList.Cell>{description}</IndexList.Cell>
        <IndexList.Cell>{status}</IndexList.Cell>
      </IndexList.Row>
    )
  }
}
