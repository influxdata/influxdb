// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {get} from 'lodash'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {Form, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import CreateScraperForm from 'src/scrapers/components/CreateScraperForm'

// Actions
import {createScraper} from 'src/scrapers/actions'

// Types
import {Bucket, ScraperTargetRequest} from '@influxdata/influx'
import {ComponentColor, ComponentStatus} from '@influxdata/clockface'
import {AppState} from 'src/types'

interface OwnProps {
  visible: boolean
}

interface StateProps {
  buckets: Bucket[]
}

interface DispatchProps {
  onCreateScraper: typeof createScraper
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

interface State {
  scraper: ScraperTargetRequest
}

class CreateScraperOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    const {
      params: {bucketID, orgID},
      buckets,
    } = this.props

    const firstBucketID = get(buckets, '0.id', '')

    this.state = {
      scraper: {
        name: 'Name this Scraper',
        type: ScraperTargetRequest.TypeEnum.Prometheus,
        url: `${this.origin}/metrics`,
        orgID,
        bucketID: bucketID ? bucketID : firstBucketID,
      },
    }
  }

  public render() {
    const {scraper} = this.state
    const {buckets} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Heading title="Create Scraper" onDismiss={this.onDismiss} />
          <Form onSubmit={this.handleSubmit}>
            <Overlay.Body>
              <h5 className="wizard-step--sub-title">
                Scrapers collect data from multiple targets at regular intervals
                and to write to a bucket
              </h5>
              <CreateScraperForm
                buckets={buckets}
                url={scraper.url}
                name={scraper.name}
                selectedBucketID={scraper.bucketID}
                onInputChange={this.handleInputChange}
                onSelectBucket={this.handleSelectBucket}
              />
            </Overlay.Body>
            <Overlay.Footer>
              <Button
                text="Cancel"
                onClick={this.onDismiss}
                testID="create-scraper--cancel"
              />
              <Button
                status={this.submitButtonStatus}
                text="Create"
                onClick={this.handleSubmit}
                color={ComponentColor.Success}
                testID="create-scraper--submit"
              />
            </Overlay.Footer>
          </Form>
        </Overlay.Container>
      </Overlay>
    )
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const scraper = {...this.state.scraper, [key]: value}

    this.setState({
      scraper,
    })
  }

  private handleSelectBucket = (bucket: Bucket) => {
    const {organizationID, id} = bucket
    const scraper = {...this.state.scraper, orgID: organizationID, bucketID: id}

    this.setState({scraper})
  }

  private get submitButtonStatus(): ComponentStatus {
    const {scraper} = this.state

    if (!scraper.url || !scraper.name || !scraper.bucketID) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
  }

  private handleSubmit = async () => {
    const {onCreateScraper} = this.props
    const {scraper} = this.state

    onCreateScraper(scraper)
    this.onDismiss()
  }

  private get origin(): string {
    return window.location.origin
  }

  private onDismiss = (): void => {
    this.props.router.goBack()
  }
}

const mstp = ({buckets}: AppState): StateProps => ({
  buckets: buckets.list,
})

const mdtp: DispatchProps = {
  onCreateScraper: createScraper,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<StateProps & DispatchProps & OwnProps>(CreateScraperOverlay))
