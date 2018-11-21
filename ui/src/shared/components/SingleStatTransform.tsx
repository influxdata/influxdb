// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Parsing
import getLastValues from 'src/shared/parsing/flux/fluxToSingleStat'

// Types
import {FluxTable} from 'src/types'

const NON_NUMERIC_ERROR =
  'Could not display single stat because your values are non-numeric'

interface Props {
  tables: FluxTable[]
  children: (stat: number) => JSX.Element
}

export default class SingleStatTransform extends PureComponent<Props> {
  public render() {
    const lastValue = +this.lastValue

    if (!_.isFinite(lastValue)) {
      return <EmptyGraphMessage message={NON_NUMERIC_ERROR} />
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
