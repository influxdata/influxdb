// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Grid, Form, Radio, ButtonShape} from 'src/clockface'
import RetentionDuration from 'src/organizations/components/RetentionDuration'

// Utils
import {
  Duration,
  durationToSeconds,
  secondsToDuration,
} from 'src/utils/formatting'

import {BucketRetentionRules} from 'src/api'

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
        <Grid.Column>
          <Form.Element label="How often to clear data?">
            <Radio shape={ButtonShape.StretchToFit}>
              <Radio.Button
                active={type === BucketRetentionRules.TypeEnum.Expire}
                onClick={this.handleRadioClick}
                value={BucketRetentionRules.TypeEnum.Expire}
              >
                Periodically
              </Radio.Button>
              <Radio.Button
                active={type === null}
                onClick={this.handleRadioClick}
                value={null}
              >
                Never
              </Radio.Button>
            </Radio>
          </Form.Element>
        </Grid.Column>
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
