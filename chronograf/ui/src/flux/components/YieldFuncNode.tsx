import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'
import TimeMachineVis from 'src/flux/components/TimeMachineVis'
import {getTimeSeries} from 'src/flux/apis'
import {getDeep} from 'src/utils/wrappers'

import {FluxTable} from 'src/types'
import {Func} from 'src/types/flux'
import {Source} from 'src/types/v2'

interface Props {
  source: Source
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

  public componentDidUpdate(prevProps: Props) {
    if (prevProps.script !== this.props.script) {
      this.getData()
    }
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
    const {script, source} = this.props
    const results = await getTimeSeries(source.links.query, script)
    const data = getDeep<FluxTable[]>(results, 'tables', [])
    this.setState({data})
  }
}

export default YieldFuncNode
