// Libraries
import React, {Component} from 'react'
import {Panel, ComponentSize, InfluxColors} from '@influxdata/clockface'

// Types
import {UsageTable} from 'src/types'

interface Props {
  title: string
  units: string
  table: UsageTable
  column: string
}

class SingleStat extends Component<Props> {
  render() {
    const {title} = this.props

    return (
      <Panel backgroundColor={InfluxColors.Onyx}>
        <Panel.Header size={ComponentSize.ExtraSmall}>
          <h5>{title}</h5>
        </Panel.Header>
        <Panel.Body size={ComponentSize.ExtraSmall}>
          <div className="usage--single-stat">{this.getSingleStat()}</div>
        </Panel.Body>
      </Panel>
    )
  }

  getSingleStat = () => {
    return `${this.getLastValue()} ${this.props.units}`
  }

  getLastValue = () => {
    const {table, column} = this.props

    try {
      const tCol = table.getColumn(column) as number[]
      const values = tCol.filter(v => !!v || v === 0) // remove empty values not 0
      return this.numberWithCommas(values[values.length - 1]) || 0
    } catch (error) {
      console.error(error, table, column)
      return 0
    }
  }
  numberWithCommas(x) {
    return x.toLocaleString()
  }
}

export default SingleStat
