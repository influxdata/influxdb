// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Parsing
import getLastValues from 'src/shared/parsing/flux/fluxToSingleStat'

// Types
import {FluxTable} from 'src/types'

interface Props {
  tables: FluxTable[]
  children: (stat: number) => JSX.Element
}

export default class SingleStatTransform extends PureComponent<Props> {
  public render() {
    const lastValue = +this.lastValue
    if (!_.isNumber(lastValue)) {
      return (
        <div>
          Could not display single stat because your values are non-numeric
        </div>
      )
    }

    return this.props.children(lastValue)
  }

  private get lastValue(): number {
    const {tables} = this.props
    const {series, values} = getLastValues(tables)
    const firstAlphabeticalSeriesName = _.sortBy(series)[0]

    const firstAlphabeticalIndex = _.indexOf(
      series,
      firstAlphabeticalSeriesName
    )

    return values[firstAlphabeticalIndex]
  }
}
