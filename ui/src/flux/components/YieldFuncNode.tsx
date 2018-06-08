import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from '../../shared/decorators/errors'

import {FluxTable} from 'src/types'
import {Func} from 'src/types/flux'
import TimeMachineVis from './TimeMachineVis'

interface Props {
  data: FluxTable[]
  index: number
  bodyID: string
  func: Func
  declarationID?: string
}

@ErrorHandling
class YieldFuncNode extends PureComponent<Props> {
  public render() {
    const {data, func} = this.props
    const yieldName = _.get(func, 'args.0.value', 'result')
    return (
      <div style={{width: '1000px', height: '1000px'}}>
        <span>{yieldName}</span>
        <TimeMachineVis data={data} />
      </div>
    )
  }
}

export default YieldFuncNode
