// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {isEmpty} from 'lodash'

// Components
import {Button, EmptyState, Sort} from '@influxdata/clockface'
import SearchWidget from 'src/shared/components/search_widget/SearchWidget'
import SettingsTabbedPageHeader from 'src/settings/components/SettingsTabbedPageHeader'
import ScraperList from 'src/scrapers/components/ScraperList'
import NoBucketsWarning from 'src/buckets/components/NoBucketsWarning'

// Actions
import {updateScraper, deleteScraper} from 'src/scrapers/actions'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {SortTypes} from 'src/shared/utils/sort'

// Types
import {ScraperTargetResponse} from '@influxdata/influx'
import {
  IconFont,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {AppState, Bucket, Organization} from 'src/types'
import FilterList from 'src/shared/components/Filter'

// Selectors
import {getOrg} from 'src/organizations/selectors'

interface StateProps {
  scrapers: ScraperTargetResponse[]
  buckets: Bucket[]
  org: Organization
}

interface DispatchProps {
  onUpdateScraper: typeof updateScraper
  onDeleteScraper: typeof deleteScraper
}

type Props = StateProps & DispatchProps & WithRouterProps

interface State {
  searchTerm: string
  sortKey: SortKey
  sortDirection: Sort
  sortType: SortTypes
}

type SortKey = keyof ScraperTargetResponse

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

    return (
      <>
        <SettingsTabbedPageHeader>
          <SearchWidget
            placeholderText="Filter scrapers..."
            searchTerm={searchTerm}
            onSearch={this.handleFilterChange}
          />
          {this.createScraperButton('create-scraper-button-header')}
        </SettingsTabbedPageHeader>
        <NoBucketsWarning visible={this.hasNoBuckets} resourceName="Scrapers" />
        <FilterList<ScraperTargetResponse>
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

  private handleUpdateScraper = (scraper: ScraperTargetResponse) => {
    const {onUpdateScraper} = this.props
    onUpdateScraper(scraper)
  }

  private handleDeleteScraper = (scraper: ScraperTargetResponse) => {
    const {onDeleteScraper} = this.props
    onDeleteScraper(scraper)
  }

  private handleShowOverlay = () => {
    const {router, org} = this.props

    if (this.hasNoBuckets) {
      return
    }

    router.push(`/orgs/${org.id}/load-data/scrapers/new`)
  }

  private handleFilterChange = (searchTerm: string) => {
    this.setState({searchTerm})
  }
}

const mstp = (state: AppState): StateProps => ({
  scrapers: state.scrapers.list,
  buckets: state.buckets.list,
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
