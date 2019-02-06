// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'

// Components
import {
  OverlayBody,
  OverlayHeading,
  ComponentStatus,
  OverlayContainer,
} from 'src/clockface'
import BucketOverlayForm from 'src/organizations/components/BucketOverlayForm'

// Types
import {Bucket, BucketRetentionRules, Organization} from '@influxdata/influx'

interface Props {
  org: Organization
  onCloseModal: () => void
  onCreateBucket: (org: Organization, bucket: Partial<Bucket>) => Promise<void>
}

interface State {
  bucket: Bucket
  ruleType: BucketRetentionRules.TypeEnum
  nameInputStatus: ComponentStatus
  nameErrorMessage: string
}

const emptyBucket = {
  name: '',
  retentionRules: [],
} as Bucket

export default class BucketOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      nameErrorMessage: '',
      bucket: emptyBucket,
      ruleType: null,
      nameInputStatus: ComponentStatus.Default,
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {bucket, nameInputStatus, nameErrorMessage, ruleType} = this.state

    return (
      <OverlayContainer maxWidth={500}>
        <OverlayHeading
          title="Create Bucket"
          onDismiss={this.props.onCloseModal}
        />
        <OverlayBody>
          <BucketOverlayForm
            name={bucket.name}
            buttonText="Create"
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

  private handleSubmit = (e: FormEvent<HTMLFormElement>): void => {
    e.preventDefault()
    this.handleCreateBucket()
  }

  private handleCreateBucket = (): void => {
    const {onCreateBucket, org} = this.props
    const orgID = org.id
    const organization = org.name

    const bucket = {
      ...this.state.bucket,
      orgID,
      organization,
    }

    onCreateBucket(org, bucket)
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
