// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import ScraperRow from 'src/scrapers/components/ScraperRow'

// Types
import {Scraper} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

type SortKey = keyof Scraper

interface Props {
  scrapers: Scraper[]
  emptyState: JSX.Element
  onDeleteScraper: (scraper) => void
  onUpdateScraper: (scraper: Scraper) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
}

export default class ScraperList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {emptyState} = this.props

    return (
      <ResourceList>
        <ResourceList.Body emptyState={emptyState}>
          {this.scrapersList}
        </ResourceList.Body>
      </ResourceList>
    )
  }

  public get scrapersList(): JSX.Element[] {
    const {
      scrapers,
      sortKey,
      sortDirection,
      sortType,
      onDeleteScraper,
      onUpdateScraper,
    } = this.props
    const sortedScrapers = this.memGetSortedResources(
      scrapers,
      sortKey,
      sortDirection,
      sortType
    )

    if (scrapers !== undefined) {
      return sortedScrapers.map(scraper => (
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
