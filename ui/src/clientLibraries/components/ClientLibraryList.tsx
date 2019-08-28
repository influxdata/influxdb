// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import LibraryCard from 'src/clientLibraries/components/LibraryCard'

// Types
import {Sort} from '@influxdata/clockface'
import {SortTypes, getSortedResources} from 'src/shared/utils/sort'

// Mocks
import {ClientLibrary} from 'src/clientLibraries/constants/mocks'

type SortKey = keyof ClientLibrary

interface Props {
  libraries: ClientLibrary[]
  emptyState: JSX.Element
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
            />
            <ResourceList.Sorter
              name="Description"
              sortKey={this.headerKeys[1]}
              sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
              onClick={onClickColumn}
            />
          </ResourceList.Header>
          <ResourceList.Body emptyState={emptyState}>
            {this.libraryList}
          </ResourceList.Body>
        </ResourceList>
      </>
    )
  }

  private get headerKeys(): SortKey[] {
    return ['name', 'description']
  }

  public get libraryList(): JSX.Element[] {
    const {libraries, sortKey, sortDirection, sortType} = this.props
    const sortedLibraries = this.memGetSortedResources(
      libraries,
      sortKey,
      sortDirection,
      sortType
    )

    if (libraries !== undefined) {
      return sortedLibraries.map(library => (
        <LibraryCard key={library.id} library={library} />
      ))
    }
  }
}
