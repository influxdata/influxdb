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
    const {bucket, onSelectBucket, url} = this.props

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <Form.Element label="Bucket">
              <Dropdown selectedID={bucket} onChange={onSelectBucket}>
                {this.dropdownBuckets}
              </Dropdown>
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <Form.Element label="Target URL">
              <Input
                type={InputType.Text}
                value={url}
                onChange={this.handleChangeURL}
                titleText="Target URL"
                size={ComponentSize.Medium}
                autoFocus={true}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private get dropdownBuckets(): JSX.Element[] {
    const {buckets} = this.props

    return buckets.map(b => (
      <Dropdown.Item key={b} value={b} id={b}>
        {b}
      </Dropdown.Item>
    ))
  }

  private handleChangeURL = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    this.props.onChangeURL(value)
  }
}

export default ScraperTarget
