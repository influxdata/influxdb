// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {Form, Input, Grid, Button, ButtonType} from '@influxdata/clockface'
import BucketDropdown from 'src/dataLoaders/components/BucketsDropdown'

// Types
import {Bucket} from 'src/types'
import {Columns, ComponentStatus, ComponentColor} from '@influxdata/clockface'

interface Props {
  buckets: Bucket[]
  name: string
  url: string
  selectedBucketID: string
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onSelectBucket: (bucket: Bucket) => void
  onSubmit: (e: FormEvent<HTMLFormElement>) => void
  onDismiss: () => void
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
      onSubmit,
      onDismiss,
    } = this.props

    return (
      <Form onSubmit={onSubmit}>
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
          <Grid.Row>
            <Grid.Column>
              <Form.Footer>
                <Button
                  text="Cancel"
                  onClick={onDismiss}
                  testID="create-scraper--cancel"
                />
                <Button
                  status={this.submitButtonStatus}
                  text="Create"
                  color={ComponentColor.Success}
                  testID="create-scraper--submit"
                  type={ButtonType.Submit}
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private get submitButtonStatus(): ComponentStatus {
    const {name, url, selectedBucketID} = this.props

    if (!url || !name || !selectedBucketID) {
      return ComponentStatus.Disabled
    }

    return ComponentStatus.Default
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
