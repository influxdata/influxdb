// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'

// Components
import {EmptyState, IndexList} from '@influxdata/clockface'
import TaskRunsRow from 'src/tasks/components/TaskRunsRow'

// Types
import {Sort, ComponentSize} from '@influxdata/clockface'
import {Run} from 'src/types'
import {SortTypes} from 'src/shared/utils/sort'

// Utils
import {getSortedResources} from 'src/shared/utils/sort'

interface Props {
  taskID: string
  runs: Run[]
  sortKey: string
  sortDirection: Sort
  sortType: SortTypes
  onClickColumn: (nextSort: Sort, sortKey: SortKey) => void
}

type SortKey = keyof Run

export default class TaskRunsList extends PureComponent<Props> {
  private memGetSortedResources = memoizeOne<typeof getSortedResources>(
    getSortedResources
  )

  public render() {
    const {sortKey, sortDirection, onClickColumn} = this.props

    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell
            columnName="Status"
            width="10%"
            sortKey={this.headerKeys[0]}
            sort={sortKey === this.headerKeys[0] ? sortDirection : Sort.None}
            onClick={onClickColumn}
          />
          <IndexList.HeaderCell
            columnName="Schedule"
            width="20%"
            sortKey={this.headerKeys[1]}
            sort={sortKey === this.headerKeys[1] ? sortDirection : Sort.None}
            onClick={onClickColumn}
          />
          <IndexList.HeaderCell
            columnName="Started"
            width="20%"
            sortKey={this.headerKeys[2]}
            sort={sortKey === this.headerKeys[2] ? sortDirection : Sort.None}
            onClick={onClickColumn}
          />
          <IndexList.HeaderCell
            columnName="Duration"
            width="20%"
            sortKey={this.headerKeys[3]}
            sort={sortKey === this.headerKeys[3] ? sortDirection : Sort.None}
            onClick={onClickColumn}
          />
          <IndexList.HeaderCell width="10%" />
        </IndexList.Header>
        <IndexList.Body
          emptyState={
            <EmptyState size={ComponentSize.Large}>
              <EmptyState.Text>
                Looks like this Task doesn't have any <b>Runs</b>
              </EmptyState.Text>
            </EmptyState>
          }
          columnCount={5}
        >
          {this.sortedRuns}
        </IndexList.Body>
      </IndexList>
    )
  }
  private get headerKeys(): SortKey[] {
    return ['status', 'scheduledFor', 'startedAt', 'duration']
  }

  private get sortedRuns(): JSX.Element[] {
    const {runs, sortKey, sortDirection, sortType, taskID} = this.props

    const sortedRuns = this.memGetSortedResources(
      runs,
      sortKey,
      sortDirection,
      sortType
    )

    const mostRecentRuns = sortedRuns.slice(0, 20)

    return mostRecentRuns.map(run => (
      <TaskRunsRow key={`run-id==${run.id}`} taskID={taskID} run={run} />
    ))
  }
}
