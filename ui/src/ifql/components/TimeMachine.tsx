import React, {PureComponent} from 'react'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'
import TimeMachineEditor from 'src/ifql/components/TimeMachineEditor'

import {Func} from 'src/ifql/components/FuncArgs'
import {OnChangeArg, OnDeleteFuncNode, OnAddNode} from 'src/types/ifql'
import {ErrorHandling} from 'src/shared/decorators/errors'

export interface Suggestion {
  name: string
  params: {
    [key: string]: string
  }
}

interface Expression {
  id: string
  funcs: Func[]
}

interface Props {
  script: string
  suggestions: Suggestion[]
  expressions: Expression[]
  onSubmitScript: () => void
  onChangeScript: (script: string) => void
  onAddNode: OnAddNode
  onChangeArg: OnChangeArg
  onDeleteFuncNode: OnDeleteFuncNode
  onGenerateScript: () => void
}

@ErrorHandling
class TimeMachine extends PureComponent<Props> {
  public render() {
    const {
      script,
      onAddNode,
      expressions,
      onChangeArg,
      onChangeScript,
      onSubmitScript,
      onDeleteFuncNode,
      onGenerateScript,
    } = this.props

    return (
      <div className="time-machine-container">
        <TimeMachineEditor
          script={script}
          onChangeScript={onChangeScript}
          onSubmitScript={onSubmitScript}
        />
        <div className="expression-container">
          {expressions.map(({funcs, id}, i) => {
            return (
              <div key={id} className="func-nodes-container">
                <h4>
                  Expression {i}
                  <FuncSelector
                    expressionID={id}
                    funcs={this.funcNames}
                    onAddNode={onAddNode}
                  />
                </h4>
                {funcs.map(func => (
                  <FuncNode
                    key={func.id}
                    func={func}
                    expressionID={id}
                    onChangeArg={onChangeArg}
                    onDelete={onDeleteFuncNode}
                    onGenerateScript={onGenerateScript}
                  />
                ))}
              </div>
            )
          })}
        </div>
      </div>
    )
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default TimeMachine
