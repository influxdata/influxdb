// Libraries
import React, {PureComponent} from 'react'

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
import {ScraperTargetResponses} from 'src/api'

interface Props {
  scrapers: ScraperTargetResponses
  onChange: () => void
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
        <ScraperList scrapers={scrapers} emptyState={this.emptyState} />
      </>
    )
  }
  private get emptyState(): JSX.Element {
    return (
      <EmptyState size={ComponentSize.Medium}>
        <EmptyState.Text text="No Scrapers match your query" />
      </EmptyState>
    )
  }
}
