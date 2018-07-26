import React, {PureComponent} from 'react'
import _ from 'lodash'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import ExpressionNode from 'src/flux/components/ExpressionNode'
import VariableNode from 'src/flux/components/VariableNode'
import FuncSelector from 'src/flux/components/FuncSelector'
import BodyDelete from 'src/flux/components/BodyDelete'
import {funcNames} from 'src/flux/constants'

import {Body, Suggestion} from 'src/types/flux'

interface Props {
  body: Body[]
  suggestions: Suggestion[]
  onAppendFrom: () => void
  onAppendJoin: () => void
  onDeleteBody: (bodyID: string) => void
}

class BodyBuilder extends PureComponent<Props> {
  public render() {
    const {body, onDeleteBody} = this.props

    const bodybuilder = body.map((b, i) => {
      if (b.declarations.length) {
        return b.declarations.map(d => {
          if (d.funcs) {
            return (
              <div className="declaration" key={i}>
                <div className="func-node--wrapper">
                  <VariableNode name={d.name} assignedToQuery={true} />
                  <div className="func-node--menu">
                    <BodyDelete bodyID={b.id} onDeleteBody={onDeleteBody} />
                  </div>
                </div>
                <ExpressionNode
                  bodyID={b.id}
                  declarationID={d.id}
                  funcNames={this.funcNames}
                  funcs={d.funcs}
                  declarationsFromBody={this.declarationsFromBody}
                  isLastBody={this.isLastBody(i)}
                  onDeleteBody={onDeleteBody}
                />
              </div>
            )
          }

          return (
            <div className="declaration" key={i}>
              <div className="func-node--wrapper">
                <VariableNode name={b.source} assignedToQuery={false} />
                <div className="func-node--menu">
                  <BodyDelete
                    bodyID={b.id}
                    type="variable"
                    onDeleteBody={onDeleteBody}
                  />
                </div>
              </div>
            </div>
          )
        })
      }

      return (
        <div className="declaration" key={i}>
          <ExpressionNode
            bodyID={b.id}
            funcs={b.funcs}
            funcNames={this.funcNames}
            declarationsFromBody={this.declarationsFromBody}
            isLastBody={this.isLastBody(i)}
            onDeleteBody={onDeleteBody}
          />
        </div>
      )
    })

    return (
      <FancyScrollbar className="body-builder--container" autoHide={true}>
        <div className="body-builder">
          {_.flatten(bodybuilder)}
          <div className="declaration">
            <FuncSelector
              bodyID="fake-body-id"
              declarationID="fake-declaration-id"
              onAddNode={this.handleCreateNewBody}
              funcs={this.newDeclarationFuncs}
              connectorVisible={false}
            />
          </div>
        </div>
      </FancyScrollbar>
    )
  }

  private isLastBody(bodyIndex: number): boolean {
    const {body} = this.props

    return bodyIndex === body.length - 1
  }

  private get newDeclarationFuncs(): string[] {
    const {body} = this.props
    const declarationFunctions = [funcNames.FROM]
    if (body.length > 1) {
      declarationFunctions.push(funcNames.JOIN)
    }
    return declarationFunctions
  }

  private get declarationsFromBody(): string[] {
    const {body} = this.props
    const declarations = _.flatten(
      body.map(b => {
        if ('declarations' in b) {
          const declarationsArray = b.declarations
          return declarationsArray.map(da => da.name)
        }
        return []
      })
    )
    return declarations
  }

  private handleCreateNewBody = name => {
    if (name === funcNames.FROM) {
      this.props.onAppendFrom()
    }
    if (name === funcNames.JOIN) {
      this.props.onAppendJoin()
    }
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default BodyBuilder
