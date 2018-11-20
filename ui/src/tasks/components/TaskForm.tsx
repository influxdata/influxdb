// Libraries
import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  ComponentSpacer,
  Form,
  Columns,
  Input,
  Radio,
  ButtonShape,
} from 'src/clockface'
import TaskOptionsOrgDropdown from 'src/tasks/components/TasksOptionsOrgDropdown'
import FluxEditor from 'src/shared/components/FluxEditor'
import TaskScheduleFormField from 'src/tasks/components/TaskScheduleFormField'

// Types
import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import {Alignment, Direction, ComponentStatus} from 'src/clockface/types'
import {Organization} from 'src/api'

interface Props {
  script: string
  orgs: Organization[]
  taskOptions: TaskOptions
  onChangeScheduleType: (schedule: TaskSchedule) => void
  onChangeScript: (script: string) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeTaskOrgID: (orgID: string) => void
}

interface State {
  retryAttempts: string
  schedule: TaskSchedule
}

export default class TaskForm extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      retryAttempts: '1',
      schedule: props.taskOptions.taskScheduleType,
    }
  }

  public render() {
    const {
      script,
      onChangeInput,
      onChangeScript,
      onChangeTaskOrgID,
      taskOptions: {name, taskScheduleType, interval, delay, cron, orgID},
      orgs,
    } = this.props

    return (
      <div className="task-page">
        <div className="task-page--options">
          <Form>
            <Form.Element label="Name" colsXS={Columns.Twelve}>
              <Input
                name="name"
                placeholder="Name your task"
                onChange={onChangeInput}
                value={name}
              />
            </Form.Element>
            <Form.Element label="Owner">
              <TaskOptionsOrgDropdown
                orgs={orgs}
                selectedOrgID={orgID}
                onChangeTaskOrgID={onChangeTaskOrgID}
              />
            </Form.Element>

            <Form.Element label="Schedule Task" colsXS={Columns.Twelve}>
              <ComponentSpacer
                align={Alignment.Left}
                direction={Direction.Vertical}
              >
                <Radio shape={ButtonShape.StretchToFit}>
                  <Radio.Button
                    id="interval"
                    active={taskScheduleType === TaskSchedule.interval}
                    value={TaskSchedule.interval}
                    titleText="Interval"
                    onClick={this.handleChangeScheduleType}
                  >
                    Interval
                  </Radio.Button>
                  <Radio.Button
                    id="cron"
                    active={taskScheduleType === TaskSchedule.cron}
                    value={TaskSchedule.cron}
                    titleText="Cron"
                    onClick={this.handleChangeScheduleType}
                  >
                    Cron
                  </Radio.Button>
                </Radio>
                <TaskScheduleFormField
                  onChangeInput={onChangeInput}
                  schedule={taskScheduleType}
                  interval={interval}
                  delay={delay}
                  cron={cron}
                />
              </ComponentSpacer>
            </Form.Element>

            <Form.Element label="Retry attempts" colsXS={Columns.Twelve}>
              <Input
                name="retry"
                placeholder=""
                onChange={this.handleChangeRetry}
                status={ComponentStatus.Disabled}
                value={this.state.retryAttempts}
              />
            </Form.Element>
          </Form>
        </div>
        <div className="task-page--editor">
          <FluxEditor
            script={script}
            onChangeScript={onChangeScript}
            visibility="visible"
            status={{text: '', type: ''}}
            suggestions={[]}
          />
        </div>
      </div>
    )
  }

  private handleChangeRetry = (e: ChangeEvent<HTMLInputElement>): void => {
    const retryAttempts = e.target.value
    this.setState({retryAttempts})
  }

  private handleChangeScheduleType = (schedule: TaskSchedule): void => {
    this.props.onChangeScheduleType(schedule)
  }
}
