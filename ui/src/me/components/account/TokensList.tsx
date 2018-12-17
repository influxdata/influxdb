// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, EmptyState, ComponentSize} from 'src/clockface'
import TokenRow from 'src/me/components/account/TokenRow'

// Actions
import {notify} from 'src/shared/actions/notifications'

// Apis
import {deleteAuthorization} from 'src/authorizations/apis/index'

// Types
import {Authorization} from 'src/api'

// Constants
import {
  TokenDeletionSuccess,
  TokenDeletionError,
} from 'src/shared/copy/notifications'

interface Props {
  auths: Authorization[]
  onNotify: typeof notify
  searchTerm: string
}

interface State {
  auths: Authorization[]
}

export default class TokenList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      auths: this.props.auths,
    }
  }

  public render() {
    const {onNotify} = this.props
    const {auths} = this.state

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Description" />
          <IndexList.HeaderCell columnName="Status" />
        </IndexList.Header>
        <IndexList.Body emptyState={this.emptyState} columnCount={2}>
          {auths.map(a => {
            return (
              <TokenRow
                key={a.id}
                auth={a}
                onNotify={onNotify}
                onDelete={this.handleDelete}
              />
            )
          })}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.props
    let emptyStateText = 'Could not find any tokens'

    if (searchTerm) {
      emptyStateText = 'Looks like no tokens match your search term'
    }
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text={emptyStateText} />
      </EmptyState>
    )
  }

  private handleDelete = async (authID: string) => {
    const {onNotify} = this.props
    const {auths} = this.state

    try {
      this.setState({
        auths: auths.filter(auth => {
          return auth.id !== authID
        }),
      })

      await deleteAuthorization(authID)

      onNotify(TokenDeletionSuccess)
    } catch (error) {
      this.setState({auths})
      onNotify(TokenDeletionError)
    }
  }
}
