// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {Form, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import CreateScraperForm from 'src/organizations/components/CreateScraperForm'

// Types
import {Bucket, ScraperTargetRequest} from '@influxdata/influx'
import {ComponentColor, ComponentStatus} from '@influxdata/clockface'

interface Props {
  visible: boolean
  buckets: Bucket[]
  onDismiss: () => void
  createScraper: (scraper: ScraperTargetRequest) => void
  overrideBucketIDSelection?: string
}

interface State {
  scraper: ScraperTargetRequest
}

class CreateScraperOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    const bucketID =
      this.props.overrideBucketIDSelection || this.props.buckets[0].id

    const orgID = this.props.buckets.find(b => b.id === bucketID).organizationID

    this.state = {
      scraper: {
        name: 'Name this Scraper',
        type: ScraperTargetRequest.TypeEnum.Prometheus,
        url: `${this.origin}/metrics`,
        orgID,
        bucketID,
      },
    }
  }

  componentDidUpdate(prevProps) {
    if (
      prevProps.visible === false &&
      this.props.visible === true &&
      this.props.overrideBucketIDSelection
    ) {
      const bucketID = this.props.overrideBucketIDSelection
      const orgID = this.props.buckets.find(b => b.id === bucketID)
        .organizationID

      const scraper = {
        ...this.state.scraper,
        bucketID,
        orgID,
      }
      this.setState({scraper})
    }
  }

  public render() {
    const {scraper} = this.state
    const {onDismiss, visible, buckets} = this.props

    return (
      <Overlay visible={visible}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Heading title="Create Scraper" onDismiss={onDismiss} />
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
                onClick={onDismiss}
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

  private handleSubmit = () => {
    const {createScraper} = this.props
    const {scraper} = this.state

    createScraper(scraper)
  }

  private get origin(): string {
    return window.location.origin
  }
}

export default CreateScraperOverlay
