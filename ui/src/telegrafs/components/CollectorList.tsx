// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'
import memoizeOne from 'memoize-one'

// Components
import {ResourceList} from '@influxdata/clockface'
import CollectorRow from 'src/telegrafs/components/CollectorCard'
import FilterList from 'src/shared/components/Filter'

// Types
import {Sort} from '@influxdata/clockface'
import {SortTypes, getSortedResources} from 'src/shared/utils/sort'
import {AppState, Telegraf} from 'src/types'
import {updateTelegraf, deleteTelegraf} from '../actions'

type SortKey = keyof Telegraf

interface OwnProps {
  emptyState: JSX.Element
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
  onFilterChange: (searchTerm: string) => void
}

interface StateProps {
  collectors: Telegraf[]
}

interface DispatchProps {
  onUpdateTelegraf: typeof updateTelegraf
  onDeleteTelegraf: typeof deleteTelegraf
}

type Props = OwnProps & StateProps & DispatchProps

class CollectorList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {emptyState, sortKey, sortDirection, onClickColumn} = this.props

    return (
      <ResourceList>
        <ResourceList.Header>
          <ResourceList.Sorter
            sortKey="name"
            sort={sortKey === 'name' ? sortDirection : Sort.None}
            name="Name"
            onClick={onClickColumn}
            testID="name-sorter"
          />
        </ResourceList.Header>
        <ResourceList.Body emptyState={emptyState}>
          {this.collectorsList}
        </ResourceList.Body>
      </ResourceList>
    )
  }

  public get collectorsList(): JSX.Element[] {
    const {
      collectors,
      sortKey,
      sortDirection,
      sortType,
      onDeleteTelegraf,
      onUpdateTelegraf,
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
          onDelete={(telegraf: Telegraf) =>
            onDeleteTelegraf(telegraf.id, telegraf.name)
          }
          onUpdate={onUpdateTelegraf}
          onFilterChange={onFilterChange}
        />
      ))
    }
  }
}

const mstp = (state: AppState): StateProps => ({
  collectors: state.telegrafs.list,
})

const mdtp: DispatchProps = {
  onUpdateTelegraf: updateTelegraf,
  onDeleteTelegraf: deleteTelegraf,
}

connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CollectorList)

type FilteredOwnProps = OwnProps & {
  searchTerm: string
}

type FilteredProps = Props & FilteredOwnProps

class FilteredCollectorList extends PureComponent<FilteredProps> {
  render() {
    const {
      searchTerm,
      collectors,
      emptyState,
      onFilterChange,
      sortKey,
      sortDirection,
      sortType,
      onClickColumn,
      onUpdateTelegraf,
      onDeleteTelegraf,
    } = this.props
    return (
      <FilterList<Telegraf>
        searchTerm={searchTerm}
        searchKeys={['metadata.buckets[]', 'name', 'labels[].name']}
        list={collectors}
      >
        {cs => (
          <CollectorList
            collectors={cs}
            emptyState={emptyState}
            onFilterChange={onFilterChange}
            sortKey={sortKey}
            sortDirection={sortDirection}
            sortType={sortType}
            onClickColumn={onClickColumn}
            onUpdateTelegraf={onUpdateTelegraf}
            onDeleteTelegraf={onDeleteTelegraf}
          />
        )}
      </FilterList>
    )
  }
}

const FilteredList = connect<StateProps, DispatchProps, FilteredOwnProps>(
  mstp,
  mdtp
)(FilteredCollectorList)

export {FilteredCollectorList, FilteredList}
