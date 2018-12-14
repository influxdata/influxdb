// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Panel, Input, Spinner} from 'src/clockface'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import TokenList from 'src/me/components/account/TokensList'
import FilterList from 'src/shared/components/Filter'

// APIs
import {getAuthorizations} from 'src/authorizations/apis'

// Types
import {Authorization} from 'src/api'

interface State {
  searchTerm: string
}

enum AuthSearchKeys {
  Description = 'description',
  Status = 'status',
}

export class Tokens extends PureComponent<{}, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state

    return (
      <Panel>
        <Panel.Body>
          <Input
            value={searchTerm}
            placeholder="Filter tokens by column"
            onChange={this.handleChangeSearchTerm}
            widthPixels={256}
          />
        </Panel.Body>
        <Panel.Body>
          <ResourceFetcher<Authorization[]> fetcher={getAuthorizations}>
            {(fetchedAuths, loading) => (
              <Spinner loading={loading}>
                <FilterList<Authorization>
                  list={fetchedAuths}
                  searchTerm={searchTerm}
                  searchKeys={this.searchKeys}
                >
                  {filteredAuths => <TokenList auths={filteredAuths} />}
                </FilterList>
              </Spinner>
            )}
          </ResourceFetcher>
        </Panel.Body>
      </Panel>
    )
  }

  private handleChangeSearchTerm = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private get searchKeys(): AuthSearchKeys[] {
    return [AuthSearchKeys.Status, AuthSearchKeys.Description]
  }
}

export default Tokens
