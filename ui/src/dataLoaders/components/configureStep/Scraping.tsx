// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Form, ComponentStatus} from 'src/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import ScraperTarget from 'src/dataLoaders/components/configureStep/ScraperTarget'

// Actions
import {
  setScraperTargetBucket,
  setScraperTargetURL,
  setScraperTargetName,
  saveScraperTarget,
} from 'src/dataLoaders/actions/dataLoaders'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'
import {notify as notifyAction, notify} from 'src/shared/actions/notifications'

// Types
import {Bucket} from '@influxdata/influx'
import {AppState} from 'src/types/v2/index'
import {
  scraperCreateSuccess,
  scraperCreateFailed,
} from 'src/shared/copy/v2/notifications'

interface OwnProps {
  onClickNext: () => void
  buckets: Bucket[]
}

interface DispatchProps {
  onSetScraperTargetBucket: typeof setScraperTargetBucket
  onSetScraperTargetURL: typeof setScraperTargetURL
  onSetScraperTargetName: typeof setScraperTargetName
  onSaveScraperTarget: typeof saveScraperTarget
  onSetBucketInfo: typeof setBucketInfo
  notify: typeof notifyAction
}

interface StateProps {
  scraperBucket: string
  url: string
  currentBucket: string
  name: string
}

type Props = OwnProps & DispatchProps & StateProps

export class Scraping extends PureComponent<Props> {
  public componentDidMount() {
    const {
      buckets,
      scraperBucket,
      currentBucket,
      onSetScraperTargetBucket,
    } = this.props

    if (!scraperBucket) {
      onSetScraperTargetBucket(currentBucket || _.get(buckets, '0.name', ''))
    }
  }

  public render() {
    const {
      scraperBucket,
      onSetScraperTargetURL,
      onSetScraperTargetName,
      url,
      buckets,
      name,
    } = this.props

    return (
      <Form onSubmit={this.handleSubmit} className="data-loading--form">
        <FancyScrollbar
          autoHide={false}
          className="data-loading--scroll-content"
        >
          <div>
            <h3 className="wizard-step--title">Add Scraper Target</h3>
            <h5 className="wizard-step--sub-title">
              Scrapers collect data from multiple targets at regular intervals
              and to write to a bucket
            </h5>
          </div>
          <ScraperTarget
            bucket={scraperBucket}
            buckets={buckets}
            onSelectBucket={this.handleSelectBucket}
            onChangeURL={onSetScraperTargetURL}
            onChangeName={onSetScraperTargetName}
            url={url}
            name={name}
          />
        </FancyScrollbar>
        <OnboardingButtons
          autoFocusNext={false}
          nextButtonStatus={this.nextButtonStatus}
          nextButtonText={'Finish'}
          className="data-loading--button-container"
        />
      </Form>
    )
  }

  private handleSelectBucket = (bucket: Bucket) => {
    const {onSetBucketInfo, onSetScraperTargetBucket} = this.props
    const {organization, organizationID, id, name} = bucket

    onSetBucketInfo(organization, organizationID, name, id)
    onSetScraperTargetBucket(bucket.name)
  }

  private get nextButtonStatus(): ComponentStatus {
    if (
      !this.props.url ||
      !this.props.name ||
      !this.props.buckets ||
      !this.props.buckets.length
    ) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }

  private handleSubmit = async () => {
    try {
      const {onSaveScraperTarget, onClickNext, notify} = this.props
      await onSaveScraperTarget()
      onClickNext()
      notify(scraperCreateSuccess())
    } catch (e) {
      console.error(e)
      notify(scraperCreateFailed())
    }
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {scraperTarget},
    steps: {bucket},
  },
}: AppState): StateProps => {
  return {
    currentBucket: bucket,
    scraperBucket: scraperTarget.bucket,
    url: scraperTarget.url,
    name: scraperTarget.name,
  }
}

const mdtp: DispatchProps = {
  onSetScraperTargetBucket: setScraperTargetBucket,
  onSetScraperTargetURL: setScraperTargetURL,
  onSaveScraperTarget: saveScraperTarget,
  onSetBucketInfo: setBucketInfo,
  onSetScraperTargetName: setScraperTargetName,
  notify: notifyAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(Scraping)
