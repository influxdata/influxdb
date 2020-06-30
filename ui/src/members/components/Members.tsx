// Libraries
import React, {PureComponent} from 'react'
// Libraries
import {isEmpty} from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router-dom'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import {EmptyState, Sort} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import MemberList from 'src/members/components/MemberList'
import FilterList from 'src/shared/components/FilterList'

// Actions
import {deleteMember} from 'src/members/actions/thunks'

// Types
import {ComponentSize} from '@influxdata/clockface'
import {AppState, Member, ResourceType} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

// Selectors
import {getAll} from 'src/resources/selectors'

interface StateProps {
  members: Member[]
}

interface DispatchProps {
  onRemoveMember: typeof deleteMember
}

type Props = StateProps & DispatchProps

interface State {
  searchTerm: string
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof Member

const FilterMembers = FilterList<Member>()

class Members extends PureComponent<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }
  public render() {
    const {searchTerm, sortKey, sortDirection, sortType} = this.state

    return (
      <>
        <TabbedPageHeader
          childrenLeft={
            <SearchWidget
              placeholderText="Filter members..."
              searchTerm={searchTerm}
              onSearch={this.handleFilterChange}
            />
          }
        />
        <FilterMembers
          list={this.props.members}
          searchKeys={['name']}
          searchTerm={searchTerm}
        >
          {ms => (
            <MemberList
              members={ms}
              emptyState={this.emptyState}
              onDelete={this.removeMember}
              sortKey={sortKey}
              sortDirection={sortDirection}
              sortType={sortType}
              onClickColumn={this.handleClickColumn}
            />
          )}
        </FilterMembers>
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private removeMember = (member: Member) => {
    const {onRemoveMember} = this.props
    onRemoveMember(member)
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text>
            Looks like there aren't any <b>Members</b>.
          </EmptyState.Text>
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text>No Members match your query</EmptyState.Text>
      </EmptyState>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const members = getAll<Member>(state, ResourceType.Members)
  return {members}
}

const mdtp: DispatchProps = {
  onRemoveMember: deleteMember,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(Members))
