// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Radio, ButtonShape} from 'src/clockface'
import RetentionDuration from 'src/organizations/components/RetentionDuration'

// Utils
import {
  Duration,
  durationToSeconds,
  secondsToDuration,
} from 'src/utils/formatting'

import 'src/organizations/components/Retention.scss'

import {BucketRetentionRules} from '@influxdata/influx'

interface Props {
  retentionSeconds: number
  type: BucketRetentionRules.TypeEnum
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (type: BucketRetentionRules.TypeEnum) => void
}

export default class Retention extends PureComponent<Props> {
  public render() {
    const {retentionSeconds, type} = this.props

    return (
      <>
        <Radio shape={ButtonShape.StretchToFit} customClass="retention--radio">
          <Radio.Button
            active={type === null}
            onClick={this.handleRadioClick}
            value={null}
          >
            Never
          </Radio.Button>
          <Radio.Button
            active={type === BucketRetentionRules.TypeEnum.Expire}
            onClick={this.handleRadioClick}
            value={BucketRetentionRules.TypeEnum.Expire}
            testID="retention-intervals"
          >
            Periodically
          </Radio.Button>
        </Radio>
        <RetentionDuration
          type={type}
          retentionSeconds={retentionSeconds}
          onChangeInput={this.handleChangeInput}
        />
      </>
    )
  }

  private handleRadioClick = (type: BucketRetentionRules.TypeEnum) => {
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
