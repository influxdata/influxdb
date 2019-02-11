// Libraries
import React, {PureComponent} from 'react'

// Types
import {Run} from '@influxdata/influx'
import {IndexList} from 'src/clockface'
import {taskRuns} from '../dummyData'
import {Page} from 'src/pageLayout'

interface Props {
  taskID: string
  runs: Run[]
}

const dummyData = taskRuns

class TaskRunsPage extends PureComponent<Props> {
  public render() {
    return (
      <>
        <Page titleTag="Runs">
          <Page.Header fullWidth={false}>
            <Page.Header.Left>
              <Page.Title title="Runs" />
            </Page.Header.Left>
            <Page.Header.Right />
          </Page.Header>
          <Page.Contents fullWidth={false} scrollable={true}>
            <div className="col-xs-12">
              <IndexList>
                <IndexList.Header>
                  <IndexList.HeaderCell columnName="Requested At" width="20%" />
                  <IndexList.HeaderCell columnName="Started At" width="20%" />
                  <IndexList.HeaderCell columnName="Finished At" width="20%" />
                  <IndexList.HeaderCell columnName="Status" width="10%" />
                  <IndexList.HeaderCell columnName="Schedule For" width="20%" />
                  <IndexList.HeaderCell columnName="" width="10%" />
                </IndexList.Header>
                <IndexList.Body emptyState={<></>} columnCount={6}>
                  {this.listRuns}
                </IndexList.Body>
              </IndexList>
            </div>
          </Page.Contents>
        </Page>
      </>
    )
  }

  public get listRuns(): JSX.Element[] {
    const taskRuns = dummyData.map(t => (
      <IndexList.Row key={`task-id--${t.id}`}>
        <IndexList.Cell>{this.dateTimeString(t.requestedAt)}</IndexList.Cell>
        <IndexList.Cell>{this.dateTimeString(t.startedAt)}</IndexList.Cell>
        <IndexList.Cell>{this.dateTimeString(t.finishedAt)}</IndexList.Cell>
        <IndexList.Cell>{t.status}</IndexList.Cell>
        <IndexList.Cell>{this.dateTimeString(t.scheduledFor)}</IndexList.Cell>
        <IndexList.Cell />
      </IndexList.Row>
    ))

    return taskRuns
  }

  private dateTimeString(dt: Date): string {
    const date = dt.toDateString()
    const time = dt.toLocaleTimeString()
    const formatted = `${date} ${time}`

    return formatted
  }
}
export default TaskRunsPage
