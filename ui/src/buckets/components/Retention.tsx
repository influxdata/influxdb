// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Radio, ButtonShape} from '@influxdata/clockface'
import RetentionDuration from 'src/buckets/components/RetentionDuration'

// Utils
import {
  Duration,
  durationToSeconds,
  secondsToDuration,
} from 'src/utils/formatting'

import {BucketRetentionRules} from '@influxdata/influx'

interface Props {
  retentionSeconds: number
  type: 'expire'
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (type: BucketRetentionRules.TypeEnum) => void
}

export const DEFAULT_SECONDS = 0

export default class Retention extends PureComponent<Props> {
  public render() {
    const {retentionSeconds, type} = this.props

    return (
      <>
        <Radio shape={ButtonShape.StretchToFit} className="retention--radio">
          <Radio.Button
            id="never"
            active={type === null}
            onClick={this.handleRadioClick}
            value={null}
            titleText="Never compress data"
          >
            Never
          </Radio.Button>
          <Radio.Button
            id="intervals"
            active={type === BucketRetentionRules.TypeEnum.Expire}
            onClick={this.handleRadioClick}
            value={BucketRetentionRules.TypeEnum.Expire}
            testID="retention-intervals"
            titleText="Compress data at regular intervals"
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
    this.props.onChangeRetentionRule(DEFAULT_SECONDS)
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
