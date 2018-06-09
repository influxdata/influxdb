import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import TimeMachineVis from 'src/flux/components/TimeMachineVis'
import {getTimeSeries} from 'src/flux/apis'
import {getDeep} from 'src/utils/wrappers'

import {FluxTable, Service} from 'src/types'
import {Func} from 'src/types/flux'

interface Props {
  service: Service
  data: FluxTable[]
  index: number
  bodyID: string
  func: Func
  declarationID?: string
  script: string
}

interface State {
  data: FluxTable[]
}

@ErrorHandling
class YieldFuncNode extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      data: [],
    }
  }

  public componentDidMount() {
    this.getData()
  }

  public render() {
    const {func} = this.props
    const {data} = this.state

    const yieldName = _.get(func, 'args.0.value', 'result')

    return (
      <div className="yield-node">
        <div className="func-node--connector" />
        <TimeMachineVis data={data} yieldName={yieldName} />
      </div>
    )
  }

  private getData = async (): Promise<void> => {
    const {script, service} = this.props
    const results = await getTimeSeries(service, script)
    const data = getDeep<FluxTable[]>(results, 'tables', [])
    this.setState({data})
  }
}

export default YieldFuncNode
