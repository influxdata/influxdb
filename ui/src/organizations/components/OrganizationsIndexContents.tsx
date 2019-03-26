// Libraries
import React, {Component} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

// Components
import {
  Alignment,
  ComponentSize,
  EmptyState,
  IndexList,
  ConfirmationButton,
} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Organization} from 'src/types'
import {deleteOrg} from 'src/organizations/actions/orgs'

interface Props {
  orgs: Organization[]
  onDeleteOrg: typeof deleteOrg
  searchTerm: string
}

@ErrorHandling
class OrganizationsPageContents extends Component<Props> {
  public render() {
    return (
      <div className="col-xs-12">
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" />
            <IndexList.HeaderCell />
          </IndexList.Header>
          <IndexList.Body columnCount={2} emptyState={this.emptyState}>
            {this.rows}
          </IndexList.Body>
        </IndexList>
      </div>
    )
  }

  private get rows(): JSX.Element[] {
    const {orgs, onDeleteOrg} = this.props
    return orgs.map(o => (
      <IndexList.Row key={o.id}>
        <IndexList.Cell>
          <Link to={`/organizations/${o.id}/members`}>{o.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <ConfirmationButton
            confirmText="Confirm"
            text="Delete"
            size={ComponentSize.ExtraSmall}
            returnValue={o}
            onConfirm={onDeleteOrg}
          />
        </IndexList.Cell>
      </IndexList.Row>
    ))
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.props

    if (searchTerm) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text text="No Organizations match your query" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="You are not a member of any Organizations" />
      </EmptyState>
    )
  }
}

export default OrganizationsPageContents
