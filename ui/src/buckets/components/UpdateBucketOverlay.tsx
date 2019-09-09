// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import BucketOverlayForm from 'src/buckets/components/BucketOverlayForm'

// Actions
import {updateBucket} from 'src/buckets/actions'

// Constants
import {DEFAULT_SECONDS} from 'src/buckets/components/Retention'

// Types
import {AppState, Bucket} from 'src/types'

interface State {
  bucket: Bucket
  ruleType: 'expire'
}

interface StateProps {
  bucket: Bucket
}

interface DispatchProps {
  onUpdateBucket: typeof updateBucket
}

type Props = StateProps & DispatchProps & WithRouterProps

class UpdateBucketOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    const {bucket} = this.props

    this.state = {
      ruleType: this.ruleType(bucket),
      bucket,
    }
  }

  public render() {
    const {bucket, ruleType} = this.state

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={500}>
          <Overlay.Header title="Edit Bucket" onDismiss={this.handleClose} />
          <Overlay.Body>
            <BucketOverlayForm
              name={bucket.name}
              buttonText="Save Changes"
              ruleType={ruleType}
              onCloseModal={this.handleClose}
              onSubmit={this.handleSubmit}
              disableRenaming={true}
              onChangeInput={this.handleChangeInput}
              retentionSeconds={this.retentionSeconds}
              onChangeRuleType={this.handleChangeRuleType}
              onChangeRetentionRule={this.handleChangeRetentionRule}
            />
          </Overlay.Body>
        </Overlay.Container>
      </Overlay>
    )
  }

  private get retentionSeconds(): number {
    const rule = this.state.bucket.retentionRules.find(r => r.type === 'expire')

    if (!rule) {
      return DEFAULT_SECONDS
    }

    return rule.everySeconds
  }

  private ruleType = (bucket: Bucket): 'expire' => {
    const rule = bucket.retentionRules.find(r => r.type === 'expire')

    if (!rule) {
      return null
    }

    return 'expire'
  }

  private handleChangeRetentionRule = (everySeconds: number): void => {
    const bucket = {
      ...this.state.bucket,
      retentionRules: [{type: 'expire' as 'expire', everySeconds}],
    }

    this.setState({bucket})
  }

  private handleChangeRuleType = (ruleType: 'expire') => {
    this.setState({ruleType})
  }

  private handleSubmit = (e): void => {
    e.preventDefault()
    const {onUpdateBucket} = this.props
    const {ruleType, bucket} = this.state

    if (ruleType === null) {
      onUpdateBucket({...bucket, retentionRules: []})
      this.handleClose()
      return
    }

    onUpdateBucket(bucket)
    this.handleClose()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const bucket = {...this.state.bucket, [key]: value}

    this.setState({bucket})
  }

  private handleClose = () => {
    const {orgID} = this.props.params
    this.props.router.push(`/orgs/${orgID}/load-data/buckets`)
  }
}

const mstp = ({buckets}: AppState, props: Props): StateProps => {
  const {
    params: {bucketID},
  } = props

  const bucket = buckets.list.find(b => b.id === bucketID)

  return {
    bucket,
  }
}

const mdtp: DispatchProps = {
  onUpdateBucket: updateBucket,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(withRouter(UpdateBucketOverlay))
