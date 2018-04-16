import React, {PureComponent} from 'react'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'

import {Func} from 'src/ifql/components/FuncArgs'
import {OnChangeArg} from 'src/ifql/components/FuncArgInput'

export interface Suggestion {
  name: string
  params: {
    [key: string]: string
  }
}

interface Props {
  script: string
  suggestions: Suggestion[]
  funcs: Func[]
  onAddNode: (name: string) => void
  onChangeScript: (script: string) => void
  onSubmitScript: (script: string) => void
  onDeleteFuncNode: (id: string) => void
  onChangeArg: OnChangeArg
}

class TimeMachine extends PureComponent<Props> {
  public render() {
    const {
      funcs,
      script,
      onAddNode,
      onChangeScript,
      onSubmitScript,
      onDeleteFuncNode,
      onChangeArg,
    } = this.props

    return (
      <div className="time-machine-container">
        <TimeMachineEditor
          script={script}
          onChangeScript={onChangeScript}
          onSubmitScript={onSubmitScript}
        />
        <div className="func-nodes-container">
          {funcs.map(f => (
            <FuncNode
              key={f.id}
              func={f}
              onChangeArg={onChangeArg}
              onDelete={onDeleteFuncNode}
            />
          ))}
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
