// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'
import {Button, EmptyState, Sort} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import MemberList from 'src/members/components/MemberList'
import FilterList from 'src/shared/components/Filter'

// Actions
import {deleteMember} from 'src/members/actions'

// Types
import {IconFont, ComponentSize, ComponentColor} from '@influxdata/clockface'
import {AppState, Member} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

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
        <SettingsTabbedPageHeader>
          <SearchWidget
            placeholderText="Filter members..."
            searchTerm={searchTerm}
            onSearch={this.handleFilterChange}
          />
          <Button
            text="Add Member"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
            onClick={this.handleOpenOverlay}
          />
        </SettingsTabbedPageHeader>
        <FilterList<Member>
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
        </FilterList>
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

  private handleOpenOverlay = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/settings/members/new`)
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`Looks like there aren't any Members , why not invite some?`}
            highlightWords={['Members']}
          />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Members match your query" />
      </EmptyState>
    )
  }
}

const mstp = ({members: {list}}: AppState): StateProps => {
  return {members: list}
}

const mdtp: DispatchProps = {
  onRemoveMember: deleteMember,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter<Props>(Members))
