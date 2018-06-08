import React, {PureComponent} from 'react'

import {FluxContext} from 'src/flux/containers/FluxPage'
import FuncSelector from 'src/flux/components/FuncSelector'
import FuncNode from 'src/flux/components/FuncNode'

import {Func} from 'src/types/flux'

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
      <FluxContext.Consumer>
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
      </FluxContext.Consumer>
    )
  }
}

export default ExpressionNode
