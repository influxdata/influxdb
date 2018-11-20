// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {ComponentSpacer, Input, InputType} from 'src/clockface'

// Types
import {Alignment} from 'src/clockface'
import {TaskSchedule} from 'src/utils/taskOptionsToFluxScript'

interface Props {
  schedule: TaskSchedule
  cron: string
  delay: string
  interval: string
  onChangeInput: (e: ChangeEvent<HTMLInputElement>) => void
}

export default class TaskScheduleFormFields extends PureComponent<Props> {
  public render() {
    const {delay, onChangeInput, interval, cron, schedule} = this.props

    return (
      <>
        <ComponentSpacer align={Alignment.Left} stretchToFit={true}>
          <label className="task-page--form-label">
            {schedule === TaskSchedule.interval ? 'Interval' : 'Cron'}
          </label>
          <Input
            name={schedule}
            type={InputType.Text}
            placeholder={
              schedule === TaskSchedule.interval ? '1d3h30s' : '0 2 * * *'
            }
            value={schedule === TaskSchedule.interval ? interval : cron}
            onChange={this.props.onChangeInput}
          />
        </ComponentSpacer>

        <ComponentSpacer align={Alignment.Left} stretchToFit={true}>
          <label className="task-page--form-label">Delay</label>
          <Input
            name="delay"
            type={InputType.Text}
            value={delay}
            placeholder="20m"
            onChange={onChangeInput}
          />
        </ComponentSpacer>
      </>
    )
  }
}
