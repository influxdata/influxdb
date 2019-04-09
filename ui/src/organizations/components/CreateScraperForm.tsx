// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {Form, Input, Grid} from '@influxdata/clockface'
import BucketDropdown from 'src/dataLoaders/components/BucketsDropdown'

// Types
import {Bucket} from '@influxdata/influx'
import {Columns} from '@influxdata/clockface'

interface Props {
  buckets: Bucket[]
  name: string
  url: string
  selectedBucketID: string
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onSelectBucket: (bucket: Bucket) => void
}

export class ScraperTarget extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {
      onInputChange,
      url,
      name,
      onSelectBucket,
      buckets,
      selectedBucketID,
    } = this.props

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthSM={Columns.Six}>
            <Form.ValidationElement
              label="Name"
              validationFunc={this.handleNameValidation}
              value={name}
            >
              {status => (
                <Input
                  value={name}
                  name="name"
                  onChange={onInputChange}
                  titleText="Name"
                  placeholder="Name this scraper"
                  autoFocus={true}
                  status={status}
                />
              )}
            </Form.ValidationElement>
          </Grid.Column>
          <Grid.Column widthSM={Columns.Six}>
            <Form.Element label="Bucket to store scraped metrics">
              <BucketDropdown
                selectedBucketID={selectedBucketID}
                buckets={buckets}
                onSelectBucket={onSelectBucket}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthSM={Columns.Twelve}>
            <Form.ValidationElement
              label="Target URL"
              validationFunc={this.handleUrlValidation}
              value={url}
            >
              {status => (
                <Input
                  value={url}
                  name="url"
                  placeholder="http://"
                  onChange={onInputChange}
                  titleText="Target URL"
                  status={status}
                />
              )}
            </Form.ValidationElement>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleNameValidation = (name: string): string | null => {
    if (!name) {
      return 'Name cannot be empty'
    }

    return null
  }

  private handleUrlValidation = (url: string): string | null => {
    if (!url) {
      return 'Target URL cannot be empty'
    }

    const isURLValid =
      _.startsWith(url, 'http://') || _.startsWith(url, 'https://')

    if (!isURLValid) {
      return 'Target URL must begin with "http://"'
    }

    return null
  }
}

export default ScraperTarget
