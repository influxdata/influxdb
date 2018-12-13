// Libraries
import React, {PureComponent} from 'react'
import memoizeOne from 'memoize-one'
import _ from 'lodash'

// Components
import EmptyGraphMessage from 'src/shared/components/EmptyGraphMessage'

// Parsing
import {lastValue} from 'src/shared/parsing/flux/lastValue'

// Types
import {FluxTable} from 'src/types'

const NON_NUMERIC_ERROR =
  'Could not display single stat because your values are non-numeric'

interface Props {
  tables: FluxTable[]
  children: (stat: number) => JSX.Element
}

export default class SingleStatTransform extends PureComponent<Props> {
  private lastValue = memoizeOne(lastValue)

  public render() {
    const {tables} = this.props
    const lastValue = this.lastValue(tables)

    if (!_.isFinite(lastValue)) {
      return <EmptyGraphMessage message={NON_NUMERIC_ERROR} />
    }

    return this.props.children(lastValue)
  }
}
