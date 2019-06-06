// Libraries
import React, {PureComponent} from 'react'

// Components
import {Form, Radio, Grid} from '@influxdata/clockface'

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
          <Radio shape={ButtonShape.StretchToFit}>
            <Radio.Button
              id="y-values-format-tab--raw"
              value=""
              active={base === ''}
              titleText="Do not format values using a unit prefix"
              onClick={onUpdateYAxisBase}
            >
              None
            </Radio.Button>
            <Radio.Button
              id="y-values-format-tab--kmb"
              value={BASE_10}
              active={base === BASE_10}
              titleText="Format values using an International System of Units prefix"
              onClick={onUpdateYAxisBase}
            >
              SI
            </Radio.Button>
            <Radio.Button
              id="y-values-format-tab--kmg"
              value={BASE_2}
              active={base === BASE_2}
              titleText="Format values using a binary unit prefix (for formatting bits or bytes)"
              onClick={onUpdateYAxisBase}
            >
              Binary
            </Radio.Button>
          </Radio>
        </Form.Element>
      </Grid.Column>
    )
  }
}

export default YAxisBase
