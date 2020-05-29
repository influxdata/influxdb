// Libraries
import React, {PureComponent} from 'react'

// Components
import {Form, SelectGroup, Grid} from '@influxdata/clockface'

// Constants
import {AXES_SCALE_OPTIONS} from 'src/dashboards/constants/cellEditor'

// Types
import {Columns, ButtonShape} from '@influxdata/clockface'

interface Props {
  scale: string
  onUpdateYAxisScale: (scale: string) => void
}

const {LINEAR, LOG} = AXES_SCALE_OPTIONS

class YAxisBase extends PureComponent<Props> {
  public render() {
    const {scale, onUpdateYAxisScale} = this.props

    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Element label="Scale">
          <SelectGroup shape={ButtonShape.StretchToFit}>
            <SelectGroup.Option
              id="y-scale-tab--linear"
              name="y-scale-tab"
              value={LINEAR}
              active={scale === LINEAR || scale === ''}
              titleText="Set Y-Axis to Linear Scale"
              onClick={onUpdateYAxisScale}
            >
              Linear
            </SelectGroup.Option>
            <SelectGroup.Option
              id="y-scale-tab--logarithmic"
              name="y-scale-tab"
              value={LOG}
              active={scale === LOG}
              titleText="Set Y-Axis to Logarithmic Scale"
              onClick={onUpdateYAxisScale}
            >
              Logarithmic
            </SelectGroup.Option>
          </SelectGroup>
        </Form.Element>
      </Grid.Column>
    )
  }
}

export default YAxisBase
