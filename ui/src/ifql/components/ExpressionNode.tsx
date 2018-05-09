import React, {PureComponent} from 'react'

import {IFQLContext} from 'src/ifql/containers/IFQLPage'
import FuncSelector from 'src/ifql/components/FuncSelector'
import FuncNode from 'src/ifql/components/FuncNode'

import {Func} from 'src/types/ifql'

interface Props {
  funcNames: any[]
  bodyID: string
  funcs: Func[]
  declarationID?: string
}

// an Expression is a group of one or more functions
class ExpressionNode extends PureComponent<Props> {
  public render() {
    const {declarationID, bodyID, funcNames, funcs} = this.props
    return (
      <IFQLContext.Consumer>
        {({onDeleteFuncNode, onAddNode, onChangeArg, onGenerateScript}) => {
          return (
            <>
              {funcs.map(func => (
                <FuncNode
                  key={func.id}
                  func={func}
                  bodyID={bodyID}
                  onChangeArg={onChangeArg}
                  onDelete={onDeleteFuncNode}
                  declarationID={declarationID}
                  onGenerateScript={onGenerateScript}
                />
              ))}
              <FuncSelector
                bodyID={bodyID}
                funcs={funcNames}
                onAddNode={onAddNode}
                declarationID={declarationID}
              />
            </>
          )
        }}
      </IFQLContext.Consumer>
    )
  }
}

export default ExpressionNode
