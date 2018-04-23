import React, {PureComponent} from 'react'
import _ from 'lodash'

import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'
import {
  FlatBody,
  OnAddNode,
  Suggestion,
  OnChangeArg,
  OnDeleteFuncNode,
} from 'src/types/ifql'

interface Props {
  body: Body[]
  onAddNode: OnAddNode
  onChangeArg: OnChangeArg
  onDeleteFuncNode: OnDeleteFuncNode
  suggestions: Suggestion[]
  onGenerateScript: () => void
}

interface Body extends FlatBody {
  id: string
}

class BodyBuilder extends PureComponent<Props> {
  public render() {
    const {
      body,
      onAddNode,
      onChangeArg,
      onDeleteFuncNode,
      onGenerateScript,
    } = this.props

    const bodybuilder = body.map(b => {
      if (b.declarations.length) {
        return b.declarations.map(d => {
          if (d.funcs) {
            return (
              <div key={d.id} className="func-nodes-container">
                <h4>
                  {d.name}
                  <FuncSelector
                    expressionID={d.id}
                    funcs={this.funcNames}
                    onAddNode={onAddNode}
                  />
                </h4>
                {d.funcs.map(func => (
                  <FuncNode
                    key={func.id}
                    func={func}
                    expressionID={func.id}
                    onChangeArg={onChangeArg}
                    onDelete={onDeleteFuncNode}
                    onGenerateScript={onGenerateScript}
                  />
                ))}
              </div>
            )
          }

          return <div key={b.id}>{b.source}</div>
        })
      }

      return (
        <div key={b.id} className="func-nodes-container">
          <h4>
            Expression
            <FuncSelector
              expressionID={b.id}
              funcs={this.funcNames}
              onAddNode={onAddNode}
            />
          </h4>
          {b.funcs.map(func => (
            <FuncNode
              key={func.id}
              func={func}
              expressionID={b.id}
              onChangeArg={onChangeArg}
              onDelete={onDeleteFuncNode}
              onGenerateScript={onGenerateScript}
            />
          ))}
        </div>
      )
    })

    return _.flatten(bodybuilder)
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default BodyBuilder
