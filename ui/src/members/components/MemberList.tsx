// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {IndexList} from 'src/clockface'
import MemberRow from 'src/members/components/MemberRow'

// Types
import {Member} from 'src/types'
import {SortTypes} from 'src/shared/selectors/sort'
import {AppState} from 'src/types'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

type SortKey = keyof Member

interface OwnProps {
  members: Member[]
  emptyState: JSX.Element
  onDelete: (member: Member) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

interface StateProps {
  sortedMembers: Member[]
}

type Props = OwnProps & StateProps

class MemberList extends PureComponent<Props> {
  public state = {
    sortedMembers: this.props.sortedMembers,
  }

  componentDidUpdate(prevProps) {
    const {members, sortedMembers, sortKey, sortDirection} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.members.length !== members.length
    ) {
      this.setState({sortedMembers})
    }
  }

  public render() {
    const {sortKey, sortDirection, onClickColumn} = this.props

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell
            sortKey={this.headerKeys[0]}
            sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
            columnName="Username"
            width="20%"
            onClick={onClickColumn}
          />
          <IndexList.HeaderCell
            sortKey={this.headerKeys[1]}
            sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
            columnName="Role"
            width="20%"
            onClick={onClickColumn}
          />
          <IndexList.HeaderCell width="60%" />
        </IndexList.Header>
        <IndexList.Body
          columnCount={3}
          emptyState={this.props.emptyState}
          data-testid="members-list"
        >
          {this.rows}
        </IndexList.Body>
      </IndexList>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'role']
  }

  private get rows(): JSX.Element[] {
    const {onDelete} = this.props
    const {sortedMembers} = this.state

    return sortedMembers.map(member => (
      <MemberRow key={member.id} member={member} onDelete={onDelete} />
    ))
  }
}

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {
    sortedMembers: getSortedResource(state.members.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(MemberList)
