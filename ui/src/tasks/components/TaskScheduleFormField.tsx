// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {ComponentSpacer, Input, InputType} from 'src/clockface'

// Types
import {Alignment} from 'src/clockface'
import {TaskSchedule} from 'src/utils/taskOptionsToFluxScript'
import TaskScheduleUnitDropdown from 'src/tasks/components/TaskScheduleUnitDropdown'

interface Props {
  schedule: TaskSchedule
  cron: string
  delayTime: string
  delayUnit: string
  intervalTime: string
  intervalUnit: string
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
  onChangeTaskScheduleUnit: (unit: string, scheduleType: string) => void
}

export default class TaskScheduleFormFields extends PureComponent<Props> {
  public render() {
    const {
      schedule,
      delayTime,
      delayUnit,
      intervalTime,
      intervalUnit,
      onChangeInput,
    } = this.props
    if (schedule === TaskSchedule.interval) {
      return (
        <>
          <ComponentSpacer align={Alignment.Left} stretchToFit={true}>
            <label className="task-page--form-label">Interval</label>
            <Input
              name="intervalTime"
              widthPixels={120}
              type={InputType.Number}
              value={intervalTime}
              onChange={this.props.onChangeInput}
            />
            <TaskScheduleUnitDropdown
              onChange={this.handleChangeUnit}
              selectedUnit={intervalUnit}
              scheduleType="intervalUnit"
            />
          </ComponentSpacer>

          <ComponentSpacer align={Alignment.Left} stretchToFit={true}>
            <label className="task-page--form-label">Delay</label>
            <Input
              name="delayTime"
              widthPixels={120}
              type={InputType.Number}
              value={delayTime}
              onChange={onChangeInput}
            />
            <TaskScheduleUnitDropdown
              onChange={this.handleChangeUnit}
              selectedUnit={delayUnit}
              scheduleType="delayUnit"
            />
          </ComponentSpacer>
        </>
      )
    }
    return (
      <>
        <Input
          name="cron"
          placeholder="02***"
          onChange={this.props.onChangeInput}
          value={this.props.cron}
        />
        <ComponentSpacer align={Alignment.Left} stretchToFit={true}>
          <label className="task-page--form-label">Delay</label>
          <Input
            name="delayTime"
            widthPixels={120}
            type={InputType.Number}
            value={this.props.delayTime}
            onChange={this.props.onChangeInput}
          />
          <TaskScheduleUnitDropdown
            onChange={this.handleChangeUnit}
            selectedUnit={this.props.delayUnit}
            scheduleType="delayUnit"
          />
        </ComponentSpacer>
      </>
    )
  }

  private handleChangeUnit = (unit: string, scheduleType: string) => {
    this.props.onChangeTaskScheduleUnit(unit, scheduleType)
  }
}
