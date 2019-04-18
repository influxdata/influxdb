// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {IndexList} from 'src/clockface'
import ScraperRow from 'src/scrapers/components/ScraperRow'

// Types
import {ScraperTargetResponse} from '@influxdata/influx'
import {AppState} from 'src/types'
import {SortTypes} from 'src/shared/selectors/sort'
import {Sort} from '@influxdata/clockface'

// Selectors
import {getSortedResource} from 'src/shared/selectors/sort'

type SortKey = keyof ScraperTargetResponse

interface OwnProps {
  scrapers: ScraperTargetResponse[]
  emptyState: JSX.Element
  onDeleteScraper: (scraper) => void
  onUpdateScraper: (scraper: ScraperTargetResponse) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

interface StateProps {
  sortedScrapers: ScraperTargetResponse[]
}

type Props = OwnProps & StateProps

class ScraperList extends PureComponent<Props> {
  public state = {
    sortedScrapers: this.props.sortedScrapers,
  }

  componentDidUpdate(prevProps) {
    const {scrapers, sortedScrapers, sortKey, sortDirection} = this.props

    if (
      prevProps.sortDirection !== sortDirection ||
      prevProps.sortKey !== sortKey ||
      prevProps.scrapers.length !== scrapers.length
    ) {
      this.setState({sortedScrapers})
    }
  }

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
    const {scrapers, onDeleteScraper, onUpdateScraper} = this.props
    const {sortedScrapers} = this.state

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

const mstp = (state: AppState, props: OwnProps): StateProps => {
  return {
    sortedScrapers: getSortedResource(state.scrapers.list, props),
  }
}

export default connect<StateProps, {}, OwnProps>(mstp)(ScraperList)
