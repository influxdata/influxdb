// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'

// Components
import {ComponentStatus, Overlay} from 'src/clockface'
import BucketOverlayForm from 'src/organizations/components/BucketOverlayForm'

// Types
import {Bucket, BucketRetentionRules, Organization} from '@influxdata/influx'

interface Props {
  orgs: Organization[]
  onCloseModal: () => void
  onCreateBucket: (bucket: Partial<Bucket>) => void
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
      <Overlay.Container maxWidth={500}>
        <Overlay.Heading
          title="Create Bucket"
          onDismiss={this.props.onCloseModal}
        />
        <Overlay.Body>
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
        </Overlay.Body>
      </Overlay.Container>
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
    let bucket

    if (everySeconds === 0) {
      bucket = {
        ...this.state.bucket,
        retentionRules: [],
      }
    } else {
      bucket = {
        ...this.state.bucket,
        retentionRules: [
          {type: BucketRetentionRules.TypeEnum.Expire, everySeconds},
        ],
      }
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
    const {onCreateBucket, orgs} = this.props

    const org = orgs[0] // TODO: display a list of orgs and have the user pick one
    const orgID = org.id
    const organization = org.name

    const bucket = {
      ...this.state.bucket,
      orgID,
      organization,
    }

    onCreateBucket(bucket)
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
