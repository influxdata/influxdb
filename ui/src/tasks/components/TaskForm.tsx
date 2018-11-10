import _ from 'lodash'
import React, {PureComponent, ChangeEvent} from 'react'

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

import {TaskOptions, TaskSchedule} from 'src/utils/taskOptionsToFluxScript'

import {Organization} from 'src/types/v2'
import {Alignment, Direction} from 'src/clockface/types'

interface Props {
  script: string
  orgs: Organization[]
  taskOptions: TaskOptions
  onChangeScheduleType: (schedule: TaskSchedule) => void
  onChangeScript: (script: string) => void
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeScheduleUnit: (unit: string, scheduleType: string) => void
  onChangeTaskOrgID: (orgID: string) => void
}

interface State {
  selectedIntervalUnit: string
  cron: string
  selectedDelayUnit: string
  delayTime: string
  retryAttempts: string
  schedule: TaskSchedule
}

export default class TaskForm extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      cron: '',
      selectedIntervalUnit: 'd',
      delayTime: '20',
      selectedDelayUnit: 'm',
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
      taskOptions: {
        name,
        taskScheduleType,
        intervalTime,
        intervalUnit,
        delayTime,
        delayUnit,
        cron,
        orgID,
      },
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
                    titleText="Interval + Delay"
                    onClick={this.handleChangeScheduleType}
                  >
                    Interval + Delay
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
                  intervalTime={intervalTime}
                  intervalUnit={intervalUnit}
                  delayTime={delayTime}
                  delayUnit={delayUnit}
                  onChangeTaskScheduleUnit={this.handleChangeTaskScheduleUnit}
                  cron={cron}
                />
              </ComponentSpacer>
            </Form.Element>

            <Form.Element label="Retry attempts" colsXS={Columns.Twelve}>
              <Input
                name="retry"
                placeholder=""
                onChange={this.handleChangeRetry}
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

  private handleChangeTaskScheduleUnit = (
    unit: string,
    scheduleType: string
  ) => {
    this.props.onChangeScheduleUnit(unit, scheduleType)
  }
}
