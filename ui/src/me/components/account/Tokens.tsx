// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input} from '@influxdata/clockface'
import TokenList from 'src/me/components/account/TokensList'
import FilterList from 'src/shared/components/Filter'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'

// Actions
import * as notifyActions from 'src/shared/actions/notifications'

// Types
import {Authorization} from '@influxdata/influx'
import {IconFont} from '@influxdata/clockface'

interface State {
  searchTerm: string
}

enum AuthSearchKeys {
  Description = 'description',
  Status = 'status',
}

interface StateProps {
  tokens: Authorization[]
}

interface DispatchProps {
  notify: typeof notifyActions.notify
}

type Props = StateProps & DispatchProps

export class Tokens extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
    const {tokens, notify} = this.props

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
              onNotify={notify}
              auths={filteredAuths}
              searchTerm={searchTerm}
            />
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

const mdtp = {
  notify: notifyActions.notify,
}

const mstp = ({tokens}) => {
  return {
    tokens: tokens.list,
  }
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(Tokens)
