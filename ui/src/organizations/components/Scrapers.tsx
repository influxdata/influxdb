// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

// APIs
import {client} from 'src/utils/api'

// Components
import ScraperList from 'src/organizations/components/ScraperList'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  ComponentStatus,
} from '@influxdata/clockface'
import {EmptyState, Input, InputType, Tabs} from 'src/clockface'
import CreateScraperOverlay from 'src/organizations/components/CreateScraperOverlay'
import NoBucketsWarning from 'src/organizations/components/NoBucketsWarning'

// Actions
import * as NotificationsActions from 'src/types/actions/notifications'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ScraperTargetResponse, Bucket} from '@influxdata/influx'
import {OverlayState} from 'src/types'
import {
  scraperDeleteSuccess,
  scraperDeleteFailed,
  scraperUpdateSuccess,
  scraperUpdateFailed,
} from 'src/shared/copy/v2/notifications'
import FilterList from 'src/shared/components/Filter'

interface Props {
  scrapers: ScraperTargetResponse[]
  onChange: () => void
  orgName: string
  buckets: Bucket[]
  notify: NotificationsActions.PublishNotificationActionCreator
}

interface State {
  overlayState: OverlayState
  searchTerm: string
}

@ErrorHandling
class Scrapers extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      overlayState: OverlayState.Closed,
      searchTerm: '',
    }
  }

  public render() {
    const {searchTerm} = this.state
    const {scrapers} = this.props

    return (
      <>
        <Tabs.TabContentsHeader>
          <Input
            icon={IconFont.Search}
            placeholder="Filter scrapers..."
            widthPixels={290}
            value={searchTerm}
            type={InputType.Text}
            onChange={this.handleFilterChange}
            onBlur={this.handleFilterBlur}
          />
          {this.createScraperButton('create-scraper-button-header')}
        </Tabs.TabContentsHeader>
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
            />
          )}
        </FilterList>
        {this.createScraperOverlay}
      </>
    )
  }

  private get hasNoBuckets(): boolean {
    const {buckets} = this.props

    if (!buckets || !buckets.length) {
      return true
    }

    return false
  }

  private get createScraperOverlay(): JSX.Element {
    const {buckets} = this.props

    if (this.hasNoBuckets) {
      return
    }

    return (
      <CreateScraperOverlay
        visible={this.isOverlayVisible}
        buckets={buckets}
        onDismiss={this.handleDismissOverlay}
      />
    )
  }

  private get isOverlayVisible(): boolean {
    return this.state.overlayState === OverlayState.Open
  }

  private handleShowOverlay = () => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleDismissOverlay = () => {
    this.setState({overlayState: OverlayState.Closed})
    this.props.onChange()
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
    const {orgName} = this.props
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`${orgName} does not own any Scrapers , why not create one?`}
            highlightWords={['Scrapers']}
          />
          {this.createScraperButton('create-scraper-button-empty')}
        </EmptyState>
      )
    }

    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Scrapers match your query" />
      </EmptyState>
    )
  }

  private handleUpdateScraper = async (scraper: ScraperTargetResponse) => {
    const {onChange, notify} = this.props
    try {
      await client.scrapers.update(scraper.id, scraper)
      onChange()
      notify(scraperUpdateSuccess(scraper.name))
    } catch (e) {
      console.error(e)
      notify(scraperUpdateFailed(scraper.name))
    }
  }

  private handleDeleteScraper = async (scraper: ScraperTargetResponse) => {
    const {onChange, notify} = this.props
    try {
      await client.scrapers.delete(scraper.id)
      onChange()
      notify(scraperDeleteSuccess(scraper.name))
    } catch (e) {
      notify(scraperDeleteFailed(scraper.name))
      console.error(e)
    }
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}

export default Scrapers
