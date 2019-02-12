// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Columns,
  Grid,
  ComponentSize,
  InputType,
  ComponentStatus,
} from 'src/clockface'
import BucketDropdown from 'src/dataLoaders/components/BucketsDropdown'
import {Bucket} from '@influxdata/influx'

interface Props {
  bucket: string
  buckets: Bucket[]
  onSelectBucket: (bucket: Bucket) => void
  onChangeURL: (value: string) => void
  onChangeName: (value: string) => void
  url: string
  name: string
}

export class ScraperTarget extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {onSelectBucket, url, name, bucket, buckets} = this.props
    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <Form.Element
              label="Name"
              errorMessage={this.nameEmpty && 'Target name is empty'}
            >
              <Input
                type={InputType.Text}
                value={name}
                onChange={this.handleChangeName}
                titleText="Name"
                size={ComponentSize.Medium}
                autoFocus={true}
                status={this.nameStatus}
              />
            </Form.Element>
            <Form.Element label="Bucket">
              <BucketDropdown
                selected={bucket}
                buckets={buckets}
                onSelectBucket={onSelectBucket}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <Form.Element
              label="Target URL"
              errorMessage={this.urlEmpty && 'Target URL is empty'}
            >
              <Input
                type={InputType.Text}
                value={url}
                onChange={this.handleChangeURL}
                titleText="Target URL"
                size={ComponentSize.Medium}
                status={this.urlStatus}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private get urlStatus(): ComponentStatus {
    if (this.urlEmpty) {
      return ComponentStatus.Error
    }
    return ComponentStatus.Default
  }

  private get urlEmpty(): boolean {
    return !this.props.url
  }

  private get nameStatus(): ComponentStatus {
    if (this.nameEmpty) {
      return ComponentStatus.Error
    }
    return ComponentStatus.Default
  }
  private get nameEmpty(): boolean {
    return !this.props.name
  }

  private handleChangeURL = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    this.props.onChangeURL(value)
  }

  private handleChangeName = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    this.props.onChangeName(value)
  }
}

export default ScraperTarget
