import React, {PureComponent} from 'react'

import MeasurementListItem from 'src/ifql/components/MeasurementListItem'

interface Props {
  measurements: string[]
}

export default class MeasurementList extends PureComponent<Props> {
  public render() {
    const {measurements} = this.props

    return measurements.map(m => (
      <MeasurementListItem key={m} measurement={m} />
    ))
  }
}
