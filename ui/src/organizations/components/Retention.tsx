// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Radio} from 'src/clockface'
import RetentionDuration from 'src/organizations/components/RetentionDuration'

// Utils
import {
  Duration,
  durationToSeconds,
  secondsToDuration,
} from 'src/utils/formatting'

import {RetentionRuleTypes} from 'src/types/v2'

interface Props {
  retentionSeconds: number
  type: RetentionRuleTypes
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (type: RetentionRuleTypes) => void
}

export default class Retention extends PureComponent<Props> {
  public render() {
    const {retentionSeconds, type} = this.props

    return (
      <>
        <Form.Element label="How often to clear data?">
          <Radio>
            <Radio.Button
              active={type === RetentionRuleTypes.Expire}
              onClick={this.handleRadioClick}
              value={RetentionRuleTypes.Expire}
            >
              Periodically
            </Radio.Button>
            <Radio.Button
              active={type === RetentionRuleTypes.Forever}
              onClick={this.handleRadioClick}
              value={RetentionRuleTypes.Forever}
            >
              Never
            </Radio.Button>
          </Radio>
        </Form.Element>
        <RetentionDuration
          type={type}
          retentionSeconds={retentionSeconds}
          onChangeInput={this.handleChangeInput}
        />
      </>
    )
  }

  private handleRadioClick = (type: RetentionRuleTypes) => {
    this.props.onChangeRuleType(type)
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {retentionSeconds} = this.props
    const value = e.target.value
    const key = e.target.name as keyof Duration
    const time = {
      ...secondsToDuration(retentionSeconds),
      [key]: Number(value),
    }

    const seconds = durationToSeconds(time)

    this.props.onChangeRetentionRule(seconds)
  }
}
