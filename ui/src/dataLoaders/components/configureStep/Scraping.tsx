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
  saveScraperTarget,
} from 'src/dataLoaders/actions/dataLoaders'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Types
import {Bucket} from 'src/api'
import {AppState} from 'src/types/v2/index'

interface OwnProps {
  onClickNext: () => void
  buckets: Bucket[]
}

interface DispatchProps {
  onSetScraperTargetBucket: typeof setScraperTargetBucket
  onSetScraperTargetURL: typeof setScraperTargetURL
  onSaveScraperTarget: typeof saveScraperTarget
  onSetBucketInfo: typeof setBucketInfo
}

interface StateProps {
  scraperBucket: string
  url: string
  currentBucket: string
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
    const {scraperBucket, onSetScraperTargetURL, url} = this.props

    return (
      <Form onSubmit={this.handleSubmit}>
        <div className="wizard-step--scroll-area">
          <FancyScrollbar autoHide={false}>
            <div className="wizard-step--scroll-content">
              <h3 className="wizard-step--title">Add Scraper Target</h3>
              <h5 className="wizard-step--sub-title">
                Scrapers collect data from multiple targets at regular intervals
                and to write to a bucket
              </h5>
              <ScraperTarget
                bucket={scraperBucket}
                buckets={this.buckets}
                onSelectBucket={this.handleSelectBucket}
                onChangeURL={onSetScraperTargetURL}
                url={url}
              />
            </div>
          </FancyScrollbar>
        </div>
        <OnboardingButtons
          autoFocusNext={false}
          nextButtonStatus={this.nextButtonStatus}
          nextButtonText={'Finish'}
        />
      </Form>
    )
  }

  private get buckets(): string[] {
    const {buckets} = this.props

    return buckets.map(b => b.name)
  }

  private handleSelectBucket = (bucket: string) => {
    const {buckets, onSetScraperTargetBucket, onSetBucketInfo} = this.props
    if (!buckets || !buckets.length) {
      return
    }

    const findBucket = buckets.find(b => b.name === bucket)
    const bucketID = findBucket.id
    const org = findBucket.organization
    const orgID = findBucket.organizationID

    onSetBucketInfo(org, orgID, bucket, bucketID)
    onSetScraperTargetBucket(bucket)
  }

  private get nextButtonStatus(): ComponentStatus {
    if (
      this.props.url === '' ||
      !this.props.buckets ||
      !this.props.buckets.length
    ) {
      return ComponentStatus.Disabled
    }
    return ComponentStatus.Default
  }

  private handleSubmit = async () => {
    const {onSaveScraperTarget, onClickNext} = this.props
    await onSaveScraperTarget()
    onClickNext()
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
  }
}

const mdtp: DispatchProps = {
  onSetScraperTargetBucket: setScraperTargetBucket,
  onSetScraperTargetURL: setScraperTargetURL,
  onSaveScraperTarget: saveScraperTarget,
  onSetBucketInfo: setBucketInfo,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(Scraping)
