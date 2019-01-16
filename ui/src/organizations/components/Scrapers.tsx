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

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'
import {ScraperTargetResponses, ScraperTargetResponse} from 'src/api'

interface Props {
  scrapers: ScraperTargetResponses
  onChange: () => void
  orgName: string
}

@ErrorHandling
export default class OrgOptions extends PureComponent<Props> {
  public render() {
    const {scrapers} = this.props
    return (
      <>
        <TabbedPageHeader>
          <h1>Scrapers</h1>
          <Button
            text="Create Scraper"
            icon={IconFont.Plus}
            color={ComponentColor.Primary}
          />
        </TabbedPageHeader>
        <ScraperList
          scrapers={scrapers}
          emptyState={this.emptyState}
          onDeleteScraper={this.handleDeleteScraper}
        />
      </>
    )
  }

  private get emptyState(): JSX.Element {
    const {orgName} = this.props
    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text
          text={`${orgName} does not own any scrapers, why not create one?`}
        />
        <Button
          text="Create Scraper"
          icon={IconFont.Plus}
          color={ComponentColor.Primary}
        />
      </EmptyState>
    )
  }

  private handleDeleteScraper = async (scraper: ScraperTargetResponse) => {
    await deleteScraper(scraper.id)
    this.props.onChange()
  }
}
