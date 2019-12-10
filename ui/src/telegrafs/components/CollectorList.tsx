// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import CollectorRow from 'src/telegrafs/components/CollectorCard'

// Types
import {ITelegraf as Telegraf} from '@influxdata/influx'
import {Sort} from '@influxdata/clockface'
import {SortTypes, getSortedResources} from 'src/shared/utils/sort'

//Utils
import {getDeep} from 'src/utils/wrappers'

type SortKey = keyof Telegraf | 'plugins.0.config.bucket'

interface Props {
  collectors: Telegraf[]
  emptyState: JSX.Element
  onDelete: (telegraf: Telegraf) => void
  onUpdate: (telegraf: Telegraf) => void
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
        <ResourceList>
          <ResourceList.Header>
            <ResourceList.Sorter
              sortKey={this.headerKeys[0]}
              sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
              name="Name"
              onClick={onClickColumn}
              testID="name-sorter"
            />
          </ResourceList.Header>
          <ResourceList.Body emptyState={emptyState}>
            {this.collectorsList}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'plugins.0.config.bucket']
  }

  public get collectorsList(): JSX.Element[] {
    const {
      collectors,
      sortKey,
      sortDirection,
      sortType,
      onDelete,
      onUpdate,
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
          onFilterChange={onFilterChange}
        />
      ))
    }
  }
}
