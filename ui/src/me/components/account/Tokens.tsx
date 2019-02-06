// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {IconFont, Input, Spinner} from 'src/clockface'
import ResourceFetcher from 'src/shared/components/resource_fetcher'
import TokenList from 'src/me/components/account/TokensList'
import FilterList from 'src/shared/components/Filter'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'

// APIs
import {client} from 'src/utils/api'

// Actions
import {notify} from 'src/shared/actions/notifications'
import {NotificationAction} from 'src/types'

// Types
import {Authorization} from '@influxdata/influx'

interface State {
  searchTerm: string
}

enum AuthSearchKeys {
  Description = 'description',
  Status = 'status',
}

interface Props {
  onNotify: NotificationAction
}

const getAuthorizations = () => client.authorizations.getAll()

export class Tokens extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {onNotify} = this.props
    const {searchTerm} = this.state

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
        <ResourceFetcher<Authorization[]> fetcher={getAuthorizations}>
          {(fetchedAuths, loading) => (
            <Spinner loading={loading}>
              <FilterList<Authorization>
                list={fetchedAuths}
                searchTerm={searchTerm}
                searchKeys={this.searchKeys}
              >
                {filteredAuths => (
                  <TokenList
                    auths={filteredAuths}
                    onNotify={onNotify}
                    searchTerm={searchTerm}
                  />
                )}
              </FilterList>
            </Spinner>
          )}
        </ResourceFetcher>
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

const mdtp = {
  onNotify: notify,
}

export default connect<Props>(
  null,
  mdtp
)(Tokens)
