// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, EmptyState, ComponentSize} from 'src/clockface'
import TokenRow from 'src/me/components/account/TokenRow'

// Types
import {Authorization} from 'src/api'

interface Props {
  auths: Authorization[]
}

export default class TokenList extends PureComponent<Props> {
  public render() {
    const {auths} = this.props
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Description" />
          <IndexList.HeaderCell columnName="Status" />
        </IndexList.Header>
        <IndexList.Body emptyState={this.emptyState} columnCount={2}>
          {auths.map(a => {
            return <TokenRow key={a.id} auth={a} />
          })}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="Looks no tokens match your search" />
      </EmptyState>
    )
  }
}
