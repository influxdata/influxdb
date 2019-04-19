// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {IndexList} from 'src/clockface'
import ScraperRow from 'src/scrapers/components/ScraperRow'

// Types
import {ScraperTargetResponse} from '@influxdata/influx'
import {SortTypes} from 'src/shared/utils/sort'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResources} from 'src/shared/utils/sort'

type SortKey = keyof ScraperTargetResponse

interface Props {
  scrapers: ScraperTargetResponse[]
  emptyState: JSX.Element
  onDeleteScraper: (scraper) => void
  onUpdateScraper: (scraper: ScraperTargetResponse) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

export default class ScraperList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {emptyState, sortKey, sortDirection, onClickColumn} = this.props

    return (
      <>
        <IndexList>
          <IndexList.Header>
            <IndexList.HeaderCell
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              columnName="Name"
              width="50%"
              onClick={onClickColumn}
            />
            <IndexList.HeaderCell
              sortKey={this.headerKeys[1]}
              sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
              columnName="Target URL"
              width="20%"
              onClick={onClickColumn}
            />
            <IndexList.HeaderCell columnName="Bucket" width="15%" />
            <IndexList.HeaderCell columnName="" width="15%" />
          </IndexList.Header>
          <IndexList.Body columnCount={4} emptyState={emptyState}>
            {this.scrapersList}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'url']
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
