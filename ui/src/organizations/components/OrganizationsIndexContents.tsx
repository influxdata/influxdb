// Libraries
import React, {Component} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

// Components
import IndexList from 'src/shared/components/index_views/IndexList'
import DeleteOrgButton from 'src/organizations/components/DeleteOrgButton'
import {Alignment, ComponentSize, EmptyState} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Organization} from 'src/types/v2'
import {deleteOrg} from 'src/organizations/actions'

interface Props {
  orgs: Organization[]
  onDeleteOrg: typeof deleteOrg
}

@ErrorHandling
class OrganizationsPageContents extends Component<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Name" />
          <IndexList.HeaderCell />
          <IndexList.HeaderCell />
        </IndexList.Header>
        <IndexList.Body columnCount={3} emptyState={this.emptyState}>
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get rows(): JSX.Element[] {
    const {orgs, onDeleteOrg} = this.props
    return orgs.map(o => (
      <IndexList.Row key={o.id}>
        <IndexList.Cell>
          <Link to={`/organizations/${o.id}/members_tab`}>{o.name}</Link>
        </IndexList.Cell>
        <IndexList.Cell>Owner</IndexList.Cell>
        <IndexList.Cell revealOnHover={true} alignment={Alignment.Right}>
          <DeleteOrgButton org={o} onDeleteOrg={onDeleteOrg} />
        </IndexList.Cell>
      </IndexList.Row>
    ))
  }

  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text text="Looks like you are not a member of any Organizations" />
      </EmptyState>
    )
  }
}

export default OrganizationsPageContents
