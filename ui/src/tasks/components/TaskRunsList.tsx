// Libraries
import React, {PureComponent} from 'react'

// Components
import {IndexList, EmptyState} from 'src/clockface'
import TaskRunsRow from 'src/tasks/components/TaskRunsRow'

// Types
import {Run} from '@influxdata/influx'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  taskID: string
  runs: Run[]
}

export default class TaskRunsList extends PureComponent<Props> {
  public render() {
    return (
      <IndexList>
        <IndexList.Header>
          <IndexList.HeaderCell columnName="Requested At" width="20%" />
          <IndexList.HeaderCell columnName="Started At" width="20%" />
          <IndexList.HeaderCell columnName="Finished At" width="20%" />
          <IndexList.HeaderCell columnName="Status" width="10%" />
          <IndexList.HeaderCell columnName="Schedule For" width="20%" />
          <IndexList.HeaderCell columnName="" width="10%" />
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
          columnCount={6}
        >
          {this.listRuns}
        </IndexList.Body>
      </IndexList>
    )
  }

  public get listRuns(): JSX.Element[] {
    const {runs, taskID} = this.props
    const taskRuns = runs.map(t => (
      <TaskRunsRow key={t.id} taskID={taskID} run={t} />
    ))

    return taskRuns
  }
}
