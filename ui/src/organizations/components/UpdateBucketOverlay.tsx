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
import {Bucket, BucketRetentionRules} from 'src/api'

interface Props {
  bucket: Bucket
  onCloseModal: () => void
  onUpdateBucket: (bucket: Bucket) => Promise<void>
}

interface State {
  bucket: Bucket
  nameErrorMessage: string
  ruleType: BucketRetentionRules.TypeEnum
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
      nameErrorMessage: '',
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {bucket, nameInputStatus, nameErrorMessage, ruleType} = this.state

    return (
      <OverlayContainer maxWidth={500}>
        <OverlayHeading
          title="Edit Bucket"
          onDismiss={this.props.onCloseModal}
        />
        <OverlayBody>
          <BucketOverlayForm
            name={bucket.name}
            buttonText="Save Changes"
            ruleType={ruleType}
            onCloseModal={onCloseModal}
            nameErrorMessage={nameErrorMessage}
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
      r => r.type === BucketRetentionRules.TypeEnum.Expire
    )

    if (!rule) {
      return 3600
    }

    return rule.everySeconds
  }

  private ruleType = (bucket: Bucket): BucketRetentionRules.TypeEnum => {
    const rule = bucket.retentionRules.find(
      r => r.type === BucketRetentionRules.TypeEnum.Expire
    )

    if (!rule) {
      return null
    }

    return BucketRetentionRules.TypeEnum.Expire
  }

  private handleChangeRetentionRule = (everySeconds: number): void => {
    const bucket = {
      ...this.state.bucket,
      retentionRules: [
        {type: BucketRetentionRules.TypeEnum.Expire, everySeconds},
      ],
    }

    this.setState({bucket})
  }

  private handleChangeRuleType = ruleType => {
    this.setState({ruleType})
  }

  private handleSubmit = (e): void => {
    e.preventDefault()
    const {onUpdateBucket} = this.props
    const {ruleType, bucket} = this.state

    if (ruleType === null) {
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
        nameErrorMessage: `Bucket ${key} cannot be empty`,
      })
    }

    this.setState({
      bucket,
      nameInputStatus: ComponentStatus.Valid,
      nameErrorMessage: '',
    })
  }
}
