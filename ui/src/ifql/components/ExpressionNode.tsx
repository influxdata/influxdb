import React, {PureComponent} from 'react'

import {IFQLContext} from 'src/ifql/containers/IFQLPage'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'

import {Func} from 'src/types/ifql'

interface Props {
  funcNames: any[]
  id: string
  funcs: Func[]
}

// an Expression is a group of one or more functions
class ExpressionNode extends PureComponent<Props> {
  public render() {
    const {id, funcNames, funcs} = this.props
    return (
      <IFQLContext.Consumer>
        {({onDeleteFuncNode, onAddNode, onChangeArg, onGenerateScript}) => {
          return (
            <div className="func-nodes-container">
              <h4>
                <FuncSelector
                  expressionID={id}
                  funcs={funcNames}
                  onAddNode={onAddNode}
                />
              </h4>
              {funcs.map(func => (
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
        }}
      </IFQLContext.Consumer>
    )
  }
}

export default ExpressionNode
