import React, {PureComponent} from 'react'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'

import {Func} from 'src/ifql/components/FuncArgs'

export interface Suggestion {
  name: string
  params: {
    [key: string]: string
  }
}

interface Props {
  suggestions: Suggestion[]
  funcs: Func[]
  onAddNode: (name: string) => void
}

class TimeMachine extends PureComponent<Props> {
  public render() {
    const {funcs, onAddNode} = this.props

    return (
      <div>
        <div className="func-nodes-container">
          {funcs.map((f, i) => <FuncNode key={i} func={f} />)}
          <FuncSelector funcs={this.funcNames} onAddNode={onAddNode} />
        </div>
      </div>
    )
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default TimeMachine
