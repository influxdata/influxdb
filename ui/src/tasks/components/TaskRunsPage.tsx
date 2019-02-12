// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Types
import {Run} from '@influxdata/influx'
import {Page} from 'src/pageLayout'
import TaskRunsList from 'src/tasks/components/TaskRunsList'

// DummyData
import {taskRuns} from 'src/tasks/dummyData'

interface Props {
  params: {id: string}
  runs: Run[]
}

const dummyData = taskRuns

class TaskRunsPage extends PureComponent<Props & WithRouterProps> {
  public render() {
    const {params} = this.props
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
              <TaskRunsList taskID={params.id} runs={dummyData} />
            </div>
          </Page.Contents>
        </Page>
      </>
    )
  }
}

export default withRouter<Props>(TaskRunsPage)
