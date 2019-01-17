// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import ScraperRow from 'src/organizations/components/ScraperRow'

// Types
import {ScraperTargetResponses, ScraperTargetResponse} from 'src/api'

// Utils
import {getDeep} from 'src/utils/wrappers'

interface Props {
  scrapers: ScraperTargetResponses
  emptyState: JSX.Element
  onDeleteScraper: (scraper) => void
}

export default class ScraperList extends PureComponent<Props> {
  public render() {
    const {emptyState} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="URL" width="50%" />
            <IndexList.HeaderCell columnName="Bucket" width="50%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {this.scrapersList}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }

  public get scrapersList(): JSX.Element[] {
    const {scrapers, onDeleteScraper} = this.props
    const scraperTargets = getDeep<ScraperTargetResponse[]>(
      scrapers,
      'scraper_targets',
      []
    )

    if (scraperTargets !== undefined) {
      return scraperTargets.map(scraper => (
        <ScraperRow
          key={scraper.id}
          scraper={scraper}
          onDeleteScraper={onDeleteScraper}
        />
      ))
    }
    return
  }
}
