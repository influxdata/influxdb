// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'
import {isEmpty} from 'lodash'

// Components
import {Button, EmptyState, Sort} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import ScraperList from 'src/scrapers/components/ScraperList'
import NoBucketsWarning from 'src/buckets/components/NoBucketsWarning'
import FilterList from 'src/shared/components/FilterList'
import ResourceSortDropdown from 'src/shared/components/resource_sort_dropdown/ResourceSortDropdown'

// Actions
import {updateScraper, deleteScraper} from 'src/scrapers/actions/thunks'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SortTypes} from 'src/shared/utils/sort'

// Types
import {
  IconFont,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {AppState, Bucket, Scraper, Organization, ResourceType} from 'src/types'
import {ScraperSortKey} from 'src/shared/components/resource_sort_dropdown/generateSortItems'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getAll} from 'src/resources/selectors'

interface StateProps {
  scrapers: Scraper[]
  buckets: Bucket[]
  org: Organization
}

interface DispatchProps {
  onUpdateScraper: typeof updateScraper
  onDeleteScraper: typeof deleteScraper
}

type Props = StateProps & DispatchProps & RouteComponentProps<{orgID: string}>

interface State {
  searchTerm: string
  sortKey: ScraperSortKey
  sortDirection: Sort
  sortType: SortTypes
}

const FilterScrapers = FilterList<Scraper>()

@ErrorHandling
class Scrapers extends PureComponent<Props, State> {
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
    const {searchTerm, sortKey, sortDirection, sortType} = this.state
    const {scrapers} = this.props

    const leftHeaderItems = (
      <>
        <SearchWidget
          placeholderText="Filter scrapers..."
          searchTerm={searchTerm}
          onSearch={this.handleFilterChange}
        />
        <ResourceSortDropdown
          resourceType={ResourceType.Scrapers}
          sortKey={sortKey}
          sortDirection={sortDirection}
          sortType={sortType}
          onSelect={this.handleSort}
        />
      </>
    )

    return (
      <>
        <TabbedPageHeader
          childrenLeft={leftHeaderItems}
          childrenRight={this.createScraperButton(
            'create-scraper-button-header'
          )}
        />
        <NoBucketsWarning visible={this.hasNoBuckets} resourceName="Scrapers" />
        <FilterScrapers
          searchTerm={searchTerm}
          searchKeys={['name', 'url']}
          list={scrapers}
        >
          {sl => (
            <ScraperList
              scrapers={sl}
              emptyState={this.emptyState}
              onDeleteScraper={this.handleDeleteScraper}
              onUpdateScraper={this.handleUpdateScraper}
              sortKey={sortKey}
              sortDirection={sortDirection}
              sortType={sortType}
            />
          )}
        </FilterScrapers>
      </>
    )
  }

  private handleSort = (
    sortKey: ScraperSortKey,
    sortDirection: Sort,
    sortType: SortTypes
  ): void => {
    this.setState({sortKey, sortDirection, sortType})
  }

  private get hasNoBuckets(): boolean {
    const {buckets} = this.props

    if (!buckets || !buckets.length) {
      return true
    }

    return false
  }

  private createScraperButton = (testID: string): JSX.Element => {
    let status = ComponentStatus.Default
    let titleText = 'Create a new Scraper'

    if (this.hasNoBuckets) {
      status = ComponentStatus.Disabled
      titleText = 'You need at least 1 bucket in order to create a scraper'
    }

    return (
      <Button
        text="Create Scraper"
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        onClick={this.handleShowOverlay}
        status={status}
        titleText={titleText}
        testID={testID}
      />
    )
  }

  private get emptyState(): JSX.Element {
    const {org} = this.props
    const {searchTerm} = this.state

    if (isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Large}>
          <EmptyState.Text>
            {`${org.name}`} does not own any <b>Scrapers</b>, why not create
            one?
          </EmptyState.Text>
          {this.createScraperButton('create-scraper-button-empty')}
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Large}>
        <EmptyState.Text>No Scrapers match your query</EmptyState.Text>
      </EmptyState>
    )
  }

  private handleUpdateScraper = (scraper: Scraper) => {
    const {onUpdateScraper} = this.props
    onUpdateScraper(scraper)
  }

  private handleDeleteScraper = (scraper: Scraper) => {
    const {onDeleteScraper} = this.props
    onDeleteScraper(scraper)
  }

  private handleShowOverlay = () => {
    const {history, org} = this.props

    if (this.hasNoBuckets) {
      return
    }

    history.push(`/orgs/${org.id}/load-data/scrapers/new`)
  }

  private handleFilterChange = (searchTerm: string) => {
    this.setState({searchTerm})
  }
}

const mstp = (state: AppState): StateProps => ({
  scrapers: getAll<Scraper>(state, ResourceType.Scrapers),
  buckets: getAll<Bucket>(state, ResourceType.Buckets),
  org: getOrg(state),
})

const mdtp: DispatchProps = {
  onDeleteScraper: deleteScraper,
  onUpdateScraper: updateScraper,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter(Scrapers))
