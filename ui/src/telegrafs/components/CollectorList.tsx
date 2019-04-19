// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import {IndexList} from 'src/clockface'
import CollectorRow from 'src/telegrafs/components/CollectorRow'

// Types
import {ITelegraf as Telegraf} from '@influxdata/influx'
import {Sort} from '@influxdata/clockface'
import {SortTypes, getSortedResources} from 'src/shared/utils/sort'

//Utils
import {getDeep} from 'src/utils/wrappers'

type SortKey = keyof Telegraf

interface Props {
  collectors: Telegraf[]
  emptyState: JSX.Element
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
  onOpenInstructions: (telegrafID: string) => void
  onFilterChange: (searchTerm: string) => void
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

export default class CollectorList extends PureComponent<Props> {
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
            <IndexList.HeaderCell columnName="Bucket" width="25%" />
            <IndexList.HeaderCell columnName="" width="25%" />
          </IndexList.Header>
          <IndexList.Body columnCount={3} emptyState={emptyState}>
            {this.collectorsList}
          </IndexList.Body>
        </IndexList>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name']
  }

  public get collectorsList(): JSX.Element[] {
    const {
      collectors,
      sortKey,
      sortDirection,
      sortType,
      onDelete,
      onUpdate,
      onOpenInstructions,
      onFilterChange,
    } = this.props
    const sortedCollectors = this.memGetSortedResources(
      collectors,
      sortKey,
      sortDirection,
      sortType
    )

    if (collectors !== undefined) {
      return sortedCollectors.map(collector => (
        <CollectorRow
          key={collector.id}
          collector={collector}
          bucket={getDeep<string>(collector, 'plugins.0.config.bucket', '')}
          onDelete={onDelete}
          onUpdate={onUpdate}
          onOpenInstructions={onOpenInstructions}
          onFilterChange={onFilterChange}
        />
      ))
    }
  }
}
