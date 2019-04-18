// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input, Sort} from '@influxdata/clockface'
import TokenList from 'src/authorizations/components/TokenList'
import FilterList from 'src/shared/components/Filter'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'

// Types
import {Authorization} from '@influxdata/influx'
import {IconFont} from '@influxdata/clockface'
import {AppState} from 'src/types'
import {SortTypes} from 'src/shared/selectors/sort'

enum AuthSearchKeys {
  Description = 'description',
  Status = 'status',
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

class TokensTab extends PureComponent<StateProps, State> {
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

    return (
      <>
        <TabbedPageHeader>
          <Input
            icon={IconFont.Search}
            value={searchTerm}
            placeholder="Filter Tokens..."
            onChange={this.handleChangeSearchTerm}
            widthPixels={256}
          />
        </TabbedPageHeader>
        <FilterList<Authorization>
          list={tokens}
          searchTerm={searchTerm}
          searchKeys={this.searchKeys}
        >
          {filteredAuths => (
            <TokenList
              auths={filteredAuths}
              searchTerm={searchTerm}
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

  private handleChangeSearchTerm = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private get searchKeys(): AuthSearchKeys[] {
    return [AuthSearchKeys.Status, AuthSearchKeys.Description]
  }
}

const mstp = ({tokens}: AppState) => ({tokens: tokens.list})

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TokensTab)
