// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  OverlayBody,
  OverlayHeading,
  ComponentStatus,
  OverlayContainer,
} from 'src/clockface'
import BucketOverlayForm from 'src/organizations/components/BucketOverlayForm'

// Types
import {Bucket, RetentionRuleTypes} from 'src/types/v2'

interface Props {
  bucket: Bucket
  onCloseModal: () => void
  onUpdateBucket: (bucket: Bucket) => Promise<void>
}

interface State {
  bucket: Bucket
  errorMessage: string
  ruleType: RetentionRuleTypes
  nameInputStatus: ComponentStatus
}

export default class BucketOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    const {bucket} = this.props
    this.state = {
      ruleType: this.ruleType(bucket),
      bucket,
      nameInputStatus: ComponentStatus.Default,
      errorMessage: '',
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {bucket, nameInputStatus, errorMessage, ruleType} = this.state

    return (
      <OverlayContainer>
        <OverlayHeading
          title="Update Bucket"
          onDismiss={this.props.onCloseModal}
        />
        <OverlayBody>
          <BucketOverlayForm
            name={bucket.name}
            buttonText="Update"
            ruleType={ruleType}
            onCloseModal={onCloseModal}
            errorMessage={errorMessage}
            onSubmit={this.handleSubmit}
            nameInputStatus={nameInputStatus}
            onChangeInput={this.handleChangeInput}
            retentionSeconds={this.retentionSeconds}
            onChangeRuleType={this.handleChangeRuleType}
            onChangeRetentionRule={this.handleChangeRetentionRule}
          />
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private get retentionSeconds(): number {
    const rule = this.state.bucket.retentionRules.find(
      r => r.type === RetentionRuleTypes.Expire
    )

    if (!rule) {
      return 0
    }

    return rule.everySeconds
  }

  private ruleType = (bucket: Bucket): RetentionRuleTypes => {
    const rule = bucket.retentionRules.find(
      r => r.type === RetentionRuleTypes.Expire
    )

    if (!rule) {
      return RetentionRuleTypes.Forever
    }

    return RetentionRuleTypes.Expire
  }

  private handleChangeRetentionRule = (everySeconds: number): void => {
    let retentionRules = []

    if (everySeconds > 0) {
      retentionRules = [{type: RetentionRuleTypes.Expire, everySeconds}]
    }

    const bucket = {...this.state.bucket, retentionRules}
    this.setState({bucket})
  }

  private handleChangeRuleType = ruleType => {
    this.setState({ruleType})
  }

  private handleSubmit = (): void => {
    const {onUpdateBucket} = this.props
    const {ruleType, bucket} = this.state

    if (ruleType === RetentionRuleTypes.Forever) {
      onUpdateBucket({...bucket, retentionRules: []})
      return
    }

    onUpdateBucket(bucket)
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    if (!value) {
      return this.setState({
        bucket,
        nameInputStatus: ComponentStatus.Error,
        errorMessage: `Bucket ${key} cannot be empty`,
      })
    }

    this.setState({
      bucket,
      nameInputStatus: ComponentStatus.Valid,
      errorMessage: '',
    })
  }
}
