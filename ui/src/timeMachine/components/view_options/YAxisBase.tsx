// Libraries
import React, {PureComponent} from 'react'

// Components
import {Form, SelectGroup, Grid} from '@influxdata/clockface'

// Constants
import {AXES_SCALE_OPTIONS} from 'src/dashboards/constants/cellEditor'

// Types
import {Columns, ButtonShape} from '@influxdata/clockface'

interface Props {
  base: string
  onUpdateYAxisBase: (base: string) => void
}

const {BASE_2, BASE_10} = AXES_SCALE_OPTIONS

class YAxisBase extends PureComponent<Props> {
  public render() {
    const {base, onUpdateYAxisBase} = this.props

    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Element label="Y-Value Unit Prefix">
          <SelectGroup shape={ButtonShape.StretchToFit}>
            <SelectGroup.Option
              name="y-values-format"
              id="y-values-format-tab--raw"
              value=""
              active={base === ''}
              titleText="Do not format values using a unit prefix"
              onClick={onUpdateYAxisBase}
            >
              None
            </SelectGroup.Option>
            <SelectGroup.Option
              name="y-values-format"
              id="y-values-format-tab--kmb"
              value={BASE_10}
              active={base === BASE_10}
              titleText="Format values using an International System of Units prefix"
              onClick={onUpdateYAxisBase}
            >
              SI
            </SelectGroup.Option>
            <SelectGroup.Option
              name="y-values-format"
              id="y-values-format-tab--kmg"
              value={BASE_2}
              active={base === BASE_2}
              titleText="Format values using a binary unit prefix (for formatting bits or bytes)"
              onClick={onUpdateYAxisBase}
            >
              Binary
            </SelectGroup.Option>
          </SelectGroup>
        </Form.Element>
      </Grid.Column>
    )
  }
}

export default YAxisBase
