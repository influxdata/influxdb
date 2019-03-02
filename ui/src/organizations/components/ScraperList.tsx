// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList} from 'src/clockface'
import ScraperRow from 'src/organizations/components/ScraperRow'

// Types
import {ScraperTargetResponse} from '@influxdata/influx'

interface Props {
  scrapers: ScraperTargetResponse[]
  emptyState: JSX.Element
  onDeleteScraper: (scraper) => void
  onUpdateScraper: (scraper: ScraperTargetResponse) => void
}

export default class ScraperList extends PureComponent<Props> {
  public render() {
    const {emptyState} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell columnName="Name" width="50%" />
            <IndexList.HeaderCell columnName="Target URL" width="20%" />
            <IndexList.HeaderCell columnName="Bucket" width="15%" />
            <IndexList.HeaderCell columnName="" width="15%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {this.scrapersList}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }

  public get scrapersList(): JSX.Element[] {
    const {scrapers, onDeleteScraper, onUpdateScraper} = this.props

    if (scrapers !== undefined) {
      return scrapers.map(scraper => (
        <ScraperRow
          key={scraper.id}
          scraper={scraper}
          onDeleteScraper={onDeleteScraper}
          onUpdateScraper={onUpdateScraper}
        />
      ))
    }
    return
  }
}
