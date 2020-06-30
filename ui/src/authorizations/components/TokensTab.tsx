// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {isEmpty} from 'lodash'

// Components
import {Sort, ComponentSize, EmptyState} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TokenList from 'src/authorizations/components/TokenList'
import FilterList from 'src/shared/components/FilterList'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import GenerateTokenDropdown from 'src/authorizations/components/GenerateTokenDropdown'
import ResourceSortDropdown from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'

// Types
import {AppState, Authorization, ResourceType} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

// Selectors
import {getAll} from 'src/resources/selectors'

enum AuthSearchKeys {
  Description = 'description',
  Status = 'status',
  CreatedAt = 'createdAt',
}

interface State {
  searchTerm: string
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

interface StateProps {
  tokens: Authorization[]
}

type SortKey = keyof Authorization

type Props = StateProps & WithRouterProps

const FilterAuthorizations = FilterList<Authorization>()

class TokensTab extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
      sortKey: 'description',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render() {
    const {searchTerm, sortKey, sortDirection, sortType} = this.state
    const {tokens} = this.props

    const leftHeaderItems = (
      <>
        <SearchWidget
          searchTerm={searchTerm}
          placeholderText="Filter Tokens..."
          onSearch={this.handleChangeSearchTerm}
          testID="input-field--filter"
        />
        <ResourceSortDropdown
          resourceType={ResourceType.Authorizations}
          sortDirection={sortDirection}
          sortKey={sortKey}
          sortType={sortType}
          onSelect={this.handleSort}
          width={238}
        />
      </>
    )

    const rightHeaderItems = <GenerateTokenDropdown />

    return (
      <>
        <TabbedPageHeader
          childrenLeft={leftHeaderItems}
          childrenRight={rightHeaderItems}
        />
        <FilterAuthorizations
          list={tokens}
          searchTerm={searchTerm}
          searchKeys={this.searchKeys}
        >
          {filteredAuths => (
            <TokenList
              auths={filteredAuths}
              emptyState={this.emptyState}
              searchTerm={searchTerm}
              sortKey={sortKey}
              sortDirection={sortDirection}
              sortType={sortType}
              onClickColumn={this.handleClickColumn}
            />
          )}
        </FilterAuthorizations>
      </>
    )
  }

  private handleSort = (
    sortKey: SortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.setState({sortKey, sortDirection, sortType})
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private handleChangeSearchTerm = (searchTerm: string): void => {
    this.setState({searchTerm})
  }

  private get searchKeys(): AuthSearchKeys[] {
    return [
      AuthSearchKeys.Status,
      AuthSearchKeys.Description,
      AuthSearchKeys.CreatedAt,
    ]
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text>
            Looks like there aren't any <b>Tokens</b>, why not generate one?
          </EmptyState.Text>
          <GenerateTokenDropdown />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text>No Tokens match your query</EmptyState.Text>
      </EmptyState>
    )
  }
}

const mstp = (state: AppState) => ({
  tokens: getAll<Authorization>(state, ResourceType.Authorizations),
})

export default connect<StateProps, {}, {}>(mstp, null)(withRouter(TokensTab))
