// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import MemberCard from 'src/members/components/MemberCard'

// Types
import {Member} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

type SortKey = keyof Member

interface Props {
  members: Member[]
  emptyState: JSX.Element
  onDelete: (member: Member) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

export default class MemberList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {sortKey, sortDirection, onClickColumn} = this.props

    return (
      <ResourceList>
        <ResourceList.Header>
          <ResourceList.Sorter
            name="Username"
            sortKey={this.headerKeys[0]}
            sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
            onClick={onClickColumn}
          />
          <ResourceList.Sorter
            name="Role"
            sortKey={this.headerKeys[1]}
            sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
            onClick={onClickColumn}
          />
        </ResourceList.Header>
        <ResourceList.Body
          emptyState={this.props.emptyState}
          data-testid="members-list"
        >
          {this.rows}
        </ResourceList.Body>
      </ResourceList>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'role']
  }

  private get rows(): JSX.Element[] {
    const {members, sortKey, sortDirection, sortType, onDelete} = this.props
    const sortedMembers = this.memGetSortedResources(
      members,
      sortKey,
      sortDirection,
      sortType
    )

    return sortedMembers.map(member => (
      <MemberCard key={member.id} member={member} onDelete={onDelete} />
    ))
  }
}
