// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {Form, Dropdown} from 'src/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Actions
import {
  setScrapingInterval,
  addScrapingURL,
  removeScrapingURL,
  updateScrapingURL,
  setScrapingBucket,
} from 'src/onboarding/actions/dataLoaders'
import {AppState} from 'src/types/v2/index'
import {SetupParams} from 'src/onboarding/apis'
import Scraper from 'src/onboarding/components/configureStep/Scraper'

interface OwnProps {
  onClickNext: () => void
  onClickBack: () => void
  onClickSkip: () => void
}

interface DispatchProps {
  setScrapingInterval: typeof setScrapingInterval
  setScrapingBucket: typeof setScrapingBucket
  addScrapingURL: typeof addScrapingURL
  removeScrapingURL: typeof removeScrapingURL
  updateScrapingURL: typeof updateScrapingURL
}

interface StateProps {
  interval: string
  bucket: string
  urls: string[]
  setupParams: SetupParams
}

type Props = OwnProps & DispatchProps & StateProps

export class Scraping extends PureComponent<Props> {
  public render() {
    const {bucket, onClickBack, onClickSkip} = this.props
    return (
      <Form onSubmit={this.handleSubmit}>
        <div className="wizard-step--scroll-area">
          <FancyScrollbar autoHide={false}>
            <div className="wizard-step--scroll-content">
              <h3 className="wizard-step--title">Add Scraper</h3>
              <h5 className="wizard-step--sub-title">
                Scrapers collect data from multiple targets at regular intervals
                and to write to a bucket
              </h5>
              <Scraper
                bucket={bucket}
                dropdownBuckets={this.dropdownBuckets}
                onChooseInterval={this.handleChange}
                onDropdownHandle={this.handleBucket}
                onAddRow={this.handleAddRow}
                onRemoveRow={this.handleRemoveRow}
                onUpdateRow={this.handleEditRow}
                tags={this.tags}
              />
            </div>
          </FancyScrollbar>
        </div>
        <OnboardingButtons
          onClickBack={onClickBack}
          onClickSkip={onClickSkip}
          showSkip={true}
          autoFocusNext={true}
          skipButtonText={'Skip'}
        />
      </Form>
    )
  }
  private get dropdownBuckets(): JSX.Element[] {
    const {setupParams} = this.props

    const buckets = [setupParams.bucket]

    // This is a hacky fix
    return buckets.map(b => (
      <Dropdown.Item key={b} value={b} id={b}>
        {b}
      </Dropdown.Item>
    ))
  }

  private handleChange = (value: string) => {
    this.props.setScrapingInterval(value)
  }

  private handleAddRow = (item: string) => {
    this.props.addScrapingURL(item)
  }

  private handleRemoveRow = (item: string) => {
    this.props.removeScrapingURL(item)
  }

  private handleEditRow = (index: number, item: string) => {
    this.props.updateScrapingURL(index, item)
  }

  private get tags(): Array<{name: string; text: string}> {
    const {urls} = this.props
    return urls.map(v => {
      return {text: v, name: v}
    })
  }

  private handleBucket = (bucket: string) => {
    this.props.setScrapingBucket(bucket)
  }

  private handleSubmit = () => {
    this.props.onClickNext()
  }
}

const mstp = ({
  onboarding: {
    steps: {setupParams},
    dataLoaders: {
      scraper: {interval, bucket, urls},
    },
  },
}: AppState) => {
  return {setupParams, interval, bucket, urls}
}

const mdtp: DispatchProps = {
  setScrapingInterval,
  setScrapingBucket,
  addScrapingURL,
  removeScrapingURL,
  updateScrapingURL,
}
export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(Scraping)
