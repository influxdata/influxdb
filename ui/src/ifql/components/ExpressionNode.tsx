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
  declarationsFromBody: string[]
}

// an Expression is a group of one or more functions
class ExpressionNode extends PureComponent<Props> {
  public render() {
    const {
      declarationID,
      bodyID,
      funcNames,
      funcs,
      declarationsFromBody,
    } = this.props
    return (
      <IFQLContext.Consumer>
        {({
          onDeleteFuncNode,
          onAddNode,
          onChangeArg,
          onGenerateScript,
          service,
        }) => {
          return (
            <>
              {funcs.map((func, i) => (
                <FuncNode
                  key={i}
                  func={func}
                  bodyID={bodyID}
                  service={service}
                  onChangeArg={onChangeArg}
                  onDelete={onDeleteFuncNode}
                  declarationID={declarationID}
                  onGenerateScript={onGenerateScript}
                  declarationsFromBody={declarationsFromBody}
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
