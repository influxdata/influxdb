// Libraries
import _ from 'lodash'
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {EmptyState, Grid, Sort} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'
import ClientLibraryList from 'src/clientLibraries/components/ClientLibraryList'
import FilterList from 'src/shared/components/Filter'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ComponentSize} from '@influxdata/clockface'
import {AppState} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

// Mocks
import {ClientLibrary} from 'src/clientLibraries/constants/mocks'

interface StateProps {
  orgName: string
}

interface OwnProps {
  libraries: ClientLibrary[]
}

type Props = OwnProps & StateProps & WithRouterProps

interface State {
  searchTerm: string
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof ClientLibrary

@ErrorHandling
class ClientLibraries extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      searchTerm: '',
      sortKey: 'name',
      sortDirection: Sort.Ascending,
      sortType: SortTypes.String,
    }
  }

  public render() {
    const {libraries} = this.props
    const {searchTerm, sortKey, sortDirection, sortType} = this.state

    return (
      <>
        <SettingsTabbedPageHeader>
          <SearchWidget
            placeholderText="Filter client libraries..."
            searchTerm={searchTerm}
            onSearch={this.handleFilterChange}
          />
        </SettingsTabbedPageHeader>
        <Grid>
          <Grid.Row>
            <Grid.Column>
              {/* Do we need the "No Buckets" warning here? */}
              <FilterList<ClientLibrary>
                searchTerm={searchTerm}
                searchKeys={['name', 'description']}
                list={libraries}
              >
                {cl => (
                  <ClientLibraryList
                    libraries={cl}
                    emptyState={this.emptyState}
                    sortKey={sortKey}
                    sortDirection={sortDirection}
                    sortType={sortType}
                    onClickColumn={this.handleClickColumn}
                  />
                )}
              </FilterList>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    const sortType = SortTypes.String
    this.setState({sortKey, sortDirection: nextSort, sortType})
  }

  private get emptyState(): JSX.Element {
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text text="This list should not be empty" />
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text
          text="No Client  Libraries  match your query"
          highlightWords={['Client', 'Libraries']}
        />
      </EmptyState>
    )
  }

  private handleFilterChange = (searchTerm: string): void => {
    this.handleFilterUpdate(searchTerm)
  }

  private handleFilterUpdate = (searchTerm: string) => {
    this.setState({searchTerm})
  }
}
const mstp = ({orgs: {org}}: AppState): StateProps => ({
  orgName: org.name,
})

export default connect<StateProps, {}>(
  mstp,
  null
)(withRouter<OwnProps>(ClientLibraries))
