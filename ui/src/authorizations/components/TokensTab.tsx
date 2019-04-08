// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input} from '@influxdata/clockface'
import TokenList from 'src/authorizations/components/TokenList'
import FilterList from 'src/shared/components/Filter'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'

// Types
import {Authorization} from '@influxdata/influx'
import {IconFont} from '@influxdata/clockface'
import {AppState} from 'src/types'

enum AuthSearchKeys {
  Description = 'description',
  Status = 'status',
}

interface State {
  searchTerm: string
}

interface StateProps {
  tokens: Authorization[]
}

class TokensTab extends PureComponent<StateProps, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
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
            <TokenList auths={filteredAuths} searchTerm={searchTerm} />
          )}
        </FilterList>
      </>
    )
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
