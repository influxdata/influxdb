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
import {Bucket, RetentionRuleTypes, Organization} from 'src/types/v2'

interface Props {
  link: string
  org: Organization
  onCloseModal: () => void
  onCreateBucket: (link: string, bucket: Partial<Bucket>) => Promise<void>
}

interface State {
  bucket: Bucket
  errorMessage: string
  ruleType: RetentionRuleTypes
  nameInputStatus: ComponentStatus
}

const emptyBucket = {
  name: '',
  retentionRules: [],
} as Bucket

export default class BucketOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      errorMessage: '',
      bucket: emptyBucket,
      ruleType: RetentionRuleTypes.Forever,
      nameInputStatus: ComponentStatus.Default,
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {bucket, nameInputStatus, errorMessage, ruleType} = this.state

    return (
      <OverlayContainer>
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
    return this.handleCreateBucket()
  }

  private handleCreateBucket = (): void => {
    const {link, onCreateBucket, org} = this.props
    const organizationID = org.id
    const organization = org.name

    const bucket = {
      ...this.state.bucket,
      organizationID,
      organization,
    }

    onCreateBucket(link, bucket)
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
