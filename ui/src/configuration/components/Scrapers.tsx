// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Input, Button, EmptyState} from '@influxdata/clockface'
import {Tabs} from 'src/clockface'
import ScraperList from 'src/organizations/components/ScraperList'
import CreateScraperOverlay from 'src/configuration/components/CreateScraperOverlay'
import NoBucketsWarning from 'src/organizations/components/NoBucketsWarning'
import FilterList from 'src/shared/components/Filter'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {
  Bucket,
  ScraperTargetRequest,
  ScraperTargetResponse,
} from '@influxdata/influx'
import {
  IconFont,
  InputType,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {OverlayState} from 'src/types'
import {AppState} from 'src/types'

//Actions
import {createScraper, updateScraper, deleteScraper} from 'src/scrapers/actions'

interface StateProps {
  buckets: Bucket[]
  scrapers: ScraperTargetResponse[]
}

interface DispatchProps {
  createScraper: typeof createScraper
  updateScraper: typeof updateScraper
  deleteScraper: typeof deleteScraper
}

interface State {
  overlayState: OverlayState
  searchTerm: string
}

type Props = StateProps & DispatchProps

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
        createScraper={this.handleCreateScraper}
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
  }

  private handleCreateScraper = async (
    scraper: ScraperTargetRequest
  ): Promise<void> => {
    try {
      await this.props.createScraper(scraper)
      this.handleDismissOverlay()
    } catch (e) {}
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
    const {searchTerm} = this.state

    if (_.isEmpty(searchTerm)) {
      return (
        <EmptyState size={ComponentSize.Medium}>
          <EmptyState.Text
            text={`Looks like you don't own any Scrapers , why not create one?`}
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
    this.props.updateScraper(scraper)
  }

  private handleDeleteScraper = async (scraper: ScraperTargetResponse) => {
    this.props.deleteScraper(scraper)
  }

  private handleFilterChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }

  private handleFilterBlur = (e: ChangeEvent<HTMLInputElement>): void => {
    this.setState({searchTerm: e.target.value})
  }
}

const mdtp: DispatchProps = {
  createScraper,
  updateScraper,
  deleteScraper,
}

const mstp = ({buckets, scrapers}: AppState): StateProps => {
  return {
    buckets: buckets.list,
    scrapers: scrapers.list,
  }
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(Scrapers)
