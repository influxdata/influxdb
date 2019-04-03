// Libraries
import React, {PureComponent} from 'react'

// Components
import {EmptyState} from '@influxdata/clockface'
import {IndexList} from 'src/clockface'
import TaskRunsRow from 'src/tasks/components/TaskRunsRow'
import SortingHat from 'src/shared/components/sorting_hat/SortingHat'

// Types
import {Sort, ComponentSize} from '@influxdata/clockface'
import {Run} from 'src/tasks/components/TaskRunsPage'

interface Props {
  taskID: string
  runs: Run[]
}

type SortKey = keyof Run

interface State {
  sortKey: SortKey
  sortDirection: Sort
}

export default class TaskRunsList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      sortKey: 'scheduledFor',
      sortDirection: Sort.Descending,
    }
  }

  public render() {
    const {sortKey, sortDirection} = this.state

    const headerKeys: SortKey[] = [
      'status',
      'scheduledFor',
      'startedAt',
      'duration',
    ]
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell
            columnName="Status"
            width="10%"
            sortKey={headerKeys[0]}
            sort={sortKey === headerKeys[0] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName="Schedule"
            width="20%"
            sortKey={headerKeys[1]}
            sort={sortKey === headerKeys[1] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName="Started"
            width="20%"
            sortKey={headerKeys[2]}
            sort={sortKey === headerKeys[2] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell
            columnName="Duration"
            width="20%"
            sortKey={headerKeys[3]}
            sort={sortKey === headerKeys[3] ? sortDirection : Sort.None}
            onClick={this.handleClickColumn}
          />
          <IndexList.HeaderCell width="10%" />
        </IndexList.Header>
        <IndexList.Body
          emptyState={
            <EmptyState size={ComponentSize.Large}>
              <EmptyState.Text
                text={"Looks like this Task doesn't have any Runs"}
                highlightWords={['Runs']}
              />
            </EmptyState>
          }
          columnCount={5}
        >
          {this.sortedRuns}
        </IndexList.Body>
      </IndexList>
    )
  }

  private handleClickColumn = (nextSort: Sort, sortKey: SortKey) => {
    this.setState({sortKey, sortDirection: nextSort})
  }

  public listRuns = (runs: Run[]): JSX.Element => {
    const {taskID} = this.props

    let recentRuns = runs.slice(0, 20)

    const runsRow = (
      <>
        {recentRuns.map(r => (
          <TaskRunsRow key={`run-id--${r.id}`} taskID={taskID} run={r} />
        ))}
      </>
    )
    return runsRow
  }

  private get sortedRuns(): JSX.Element {
    const {runs} = this.props
    const {sortKey, sortDirection} = this.state

    if (runs.length) {
      return (
        <SortingHat<Run>
          list={runs}
          sortKey={sortKey}
          direction={sortDirection}
        >
          {this.listRuns}
        </SortingHat>
      )
    }

    return null
  }
}
