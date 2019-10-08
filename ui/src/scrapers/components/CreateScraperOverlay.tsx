// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import {get} from 'lodash'
import {connect} from 'react-redux'

// Components
import {Overlay} from '@influxdata/clockface'
import CreateScraperForm from 'src/scrapers/components/CreateScraperForm'

// Actions
import {createScraper} from 'src/scrapers/actions'

// Types
import {ScraperTargetRequest} from '@influxdata/influx'
import {AppState, Bucket} from 'src/types'

interface OwnProps {
  onDismiss: () => void
  bucketID?: string
}

interface StateProps {
  buckets: Bucket[]
  orgID: string
}

interface DispatchProps {
  onCreateScraper: typeof createScraper
}

type Props = OwnProps & StateProps & DispatchProps

interface State {
  scraper: ScraperTargetRequest
}

class CreateScraperOverlay extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    const {bucketID, orgID, buckets} = this.props

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
    const {buckets, onDismiss} = this.props

    return (
      <Overlay visible={true}>
        <Overlay.Container maxWidth={600}>
          <Overlay.Header title="Create Scraper" onDismiss={onDismiss} />
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
              onSubmit={this.handleFormSubmit}
              onDismiss={onDismiss}
            />
          </Overlay.Body>
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
    const {orgID, id} = bucket

    const scraper = {...this.state.scraper, orgID: orgID, bucketID: id}

    this.setState({scraper})
  }

  private handleFormSubmit = (e: FormEvent<HTMLFormElement>): void => {
    const {onDismiss, onCreateScraper} = this.props
    const {scraper} = this.state
    e.preventDefault()
    onCreateScraper(scraper)
    onDismiss()
  }

  private get origin(): string {
    return window.location.origin
  }
}

const mstp = ({buckets, orgs: {org}}: AppState): StateProps => ({
  buckets: buckets.list,
  orgID: org.id,
})

const mdtp: DispatchProps = {
  onCreateScraper: createScraper,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(CreateScraperOverlay)
