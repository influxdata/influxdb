// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SelectGroup, ButtonShape} from '@influxdata/clockface'
import DurationSelector, {
  DurationOption,
} from 'src/shared/components/DurationSelector'

// Utils
import {parseDuration, durationToMilliseconds} from 'src/shared/utils/duration'
import {extractBucketMaxRetentionSeconds} from 'src/cloud/utils/limits'
import {ruleToString} from 'src/utils/formatting'

// Types
import {AppState} from 'src/types'

export const DEFAULT_SECONDS = 30 * 24 * 60 * 60 // 30 days
export const READABLE_DEFAULT_SECONDS = ruleToString(DEFAULT_SECONDS)

export const DURATION_OPTIONS: DurationOption[] = [
  {duration: '1h', displayText: '1 hour'},
  {duration: '6h', displayText: '6 hours'},
  {duration: '12h', displayText: '12 hours'},
  {duration: '24h', displayText: '24 hours'},
  {duration: '48h', displayText: '48 hours'},
  {duration: '72h', displayText: '72 hours'},
  {duration: '7d', displayText: '7 days'},
  {duration: '14d', displayText: '14 days'},
  {duration: '30d', displayText: '30 days'},
  {duration: '90d', displayText: '90 days'},
  {duration: '1y', displayText: '1 year'},
]

interface StateProps {
  maxRetentionSeconds: number
}

interface OwnProps {
  retentionSeconds: number
  type: 'expire'
  onChangeRetentionRule: (seconds: number) => void
  onChangeRuleType: (type: 'expire' | null) => void
}

type Props = OwnProps & StateProps

class Retention extends PureComponent<Props> {
  public render() {
    const {retentionSeconds, maxRetentionSeconds, type} = this.props

    return (
      <>
        <SelectGroup
          shape={ButtonShape.StretchToFit}
          className="retention--radio"
        >
          <SelectGroup.Option
            name="bucket-retention"
            id="never"
            testID="retention-never--button"
            active={type === null}
            onClick={this.handleRadioClick}
            value={null}
            titleText="Never delete data"
            disabled={!!maxRetentionSeconds}
          >
            Never
          </SelectGroup.Option>
          <SelectGroup.Option
            name="bucket-retention"
            id="intervals"
            active={type === 'expire'}
            onClick={this.handleRadioClick}
            value="expire"
            testID="retention-intervals--button"
            titleText="Delete data older than a duration"
          >
            Older Than
          </SelectGroup.Option>
        </SelectGroup>
        {type === 'expire' && (
          <DurationSelector
            selectedDuration={`${retentionSeconds}s`}
            onSelectDuration={this.handleSelectDuration}
            durations={this.durations}
          />
        )}
      </>
    )
  }

  private get durations() {
    const {maxRetentionSeconds} = this.props

    if (!maxRetentionSeconds) {
      return DURATION_OPTIONS
    }

    return DURATION_OPTIONS.filter(
      ({duration}) =>
        durationToMilliseconds(parseDuration(duration)) <=
        maxRetentionSeconds * 1000
    )
  }

  private handleRadioClick = (type: 'expire' | null) => {
    this.props.onChangeRuleType(type)
  }

  private handleSelectDuration = (durationStr: string) => {
    const durationSeconds =
      durationToMilliseconds(parseDuration(durationStr)) / 1000

    this.props.onChangeRetentionRule(durationSeconds)
  }
}

const mstp = (state: AppState): StateProps => ({
  maxRetentionSeconds: extractBucketMaxRetentionSeconds(state.cloud.limits),
})

export default connect<StateProps, {}, OwnProps>(mstp)(Retention)
