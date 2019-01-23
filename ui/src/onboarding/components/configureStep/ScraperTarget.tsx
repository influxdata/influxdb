// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Columns,
  Grid,
  ComponentSize,
  Dropdown,
  InputType,
  ComponentStatus,
} from 'src/clockface'

interface Props {
  bucket: string
  buckets: string[]
  onSelectBucket: (value: string) => void
  onChangeURL: (value: string) => void
  url: string
}

export class ScraperTarget extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {onSelectBucket, url} = this.props
    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <Form.Element label="Bucket">
              <Dropdown
                selectedID={this.selectedBucket}
                onChange={onSelectBucket}
              >
                {this.dropdownBuckets}
              </Dropdown>
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <Form.Element
              label="Target URL"
              errorMessage={this.urlEmpty && 'target URL is empty'}
            >
              <Input
                type={InputType.Text}
                value={url}
                onChange={this.handleChangeURL}
                titleText="Target URL"
                size={ComponentSize.Medium}
                autoFocus={true}
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

  private get selectedBucket(): string {
    const {buckets, bucket} = this.props

    if (!buckets || !buckets.length) {
      return 'empty'
    }
    return bucket
  }

  private get dropdownBuckets(): JSX.Element[] {
    const {buckets} = this.props
    if (!buckets || !buckets.length) {
      return [
        <Dropdown.Item key={'none'} value={'No buckets found'} id={'empty'}>
          {'No buckets found'}
        </Dropdown.Item>,
      ]
    }

    return buckets.map(b => (
      <Dropdown.Item key={b} value={b} id={b}>
        {b}
      </Dropdown.Item>
    ))
  }

  private get urlEmpty(): boolean {
    return !this.props.url
  }

  private handleChangeURL = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    this.props.onChangeURL(value)
  }
}

export default ScraperTarget
