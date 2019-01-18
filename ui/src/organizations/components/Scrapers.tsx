// Libraries
import React, {PureComponent} from 'react'

// APIs
import {deleteScraper} from 'src/organizations/apis/index'

// Components
import TabbedPageHeader from 'src/shared/components/tabbed_page/TabbedPageHeader'
import ScraperList from 'src/organizations/components/ScraperList'
import {
  Button,
  ComponentColor,
  IconFont,
  ComponentSize,
  EmptyState,
} from 'src/clockface'
import DataLoadersWizard from 'src/dataLoaders/components/DataLoadersWizard'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {ScraperTargetResponses, ScraperTargetResponse, Bucket} from 'src/api'
import {OverlayState} from 'src/types/v2'
import {DataLoaderType, DataLoaderStep} from 'src/types/v2/dataLoaders'

interface Props {
  scrapers: ScraperTargetResponses
  onChange: () => void
  orgName: string
  buckets: Bucket[]
}

interface State {
  overlayState: OverlayState
}

@ErrorHandling
export default class Scrapers extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {overlayState: OverlayState.Closed}
  }

  public render() {
    const {scrapers, buckets} = this.props
    return (
      <>
        <TabbedPageHeader>
          <h1>Scrapers</h1>
          {this.createScraperButton}
        </TabbedPageHeader>
        <ScraperList
          scrapers={scrapers}
          emptyState={this.emptyState}
          onDeleteScraper={this.handleDeleteScraper}
        />
        <DataLoadersWizard
          visible={this.isOverlayVisible}
          onCompleteSetup={this.handleDismissDataLoaders}
          startingType={DataLoaderType.Scraping}
          startingStep={DataLoaderStep.Configure}
          buckets={buckets}
        />
      </>
    )
  }

  private get isOverlayVisible(): boolean {
    return this.state.overlayState === OverlayState.Open
  }

  private get createScraperButton(): JSX.Element {
    return (
      <Button
        text="Create Scraper"
        icon={IconFont.Plus}
        color={ComponentColor.Primary}
        onClick={this.handleAddScraper}
      />
    )
  }

  private handleAddScraper = () => {
    this.setState({overlayState: OverlayState.Open})
  }

  private handleDismissDataLoaders = () => {
    this.setState({overlayState: OverlayState.Closed})
    this.props.onChange()
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text
          text={`${orgName} does not own any Scrapers , why not create one?`}
          highlightWords={['Scrapers']}
        />
        {this.createScraperButton}
      </EmptyState>
    )
  }

  private handleDeleteScraper = async (scraper: ScraperTargetResponse) => {
    await deleteScraper(scraper.id)
    this.props.onChange()
  }
}
