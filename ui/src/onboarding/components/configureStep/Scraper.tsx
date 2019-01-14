// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Columns,
  Grid,
  ComponentSize,
  MultipleInput,
  Dropdown,
  InputType,
} from 'src/clockface'

interface Props {
  bucket: string
  dropdownBuckets: JSX.Element[]
  onChooseInterval: (value: string) => void
  onDropdownHandle: (value: string) => void
  onAddRow: (item: string) => void
  onRemoveRow: (item: string) => void
  onUpdateRow: (index: number, item: string) => void
  tags: Array<{name: string; text: string}>
}

interface State {
  intervalInput: string
}

export class Scraper extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      intervalInput: '',
    }
  }
  public render() {
    const {intervalInput} = this.state
    const {
      bucket,
      dropdownBuckets,
      onDropdownHandle,
      onAddRow,
      onRemoveRow,
      onUpdateRow,
      tags,
    } = this.props

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column
            widthXS={Columns.Six}
            widthMD={Columns.Five}
            offsetMD={Columns.One}
          >
            <Form.Element label="Interval">
              <Input
                type={InputType.Text}
                value={intervalInput}
                onChange={this.handleChange}
                titleText="Interval"
                size={ComponentSize.Medium}
                autoFocus={true}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column
            widthXS={Columns.Six}
            widthMD={Columns.Five}
            offsetMD={Columns.One}
          >
            <Form.Element label="Bucket">
              <Dropdown selectedID={bucket} onChange={onDropdownHandle}>
                {dropdownBuckets}
              </Dropdown>
            </Form.Element>
          </Grid.Column>
          <Grid.Column
            widthXS={Columns.Twelve}
            widthMD={Columns.Ten}
            offsetMD={Columns.One}
          >
            <MultipleInput
              onAddRow={onAddRow}
              onDeleteRow={onRemoveRow}
              onEditRow={onUpdateRow}
              tags={tags}
              title={'Add URL'}
              helpText={''}
            />
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    this.setState({intervalInput: value})
    this.props.onChooseInterval(value)
  }
}
export default Scraper
