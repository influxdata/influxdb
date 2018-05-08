import React, {PureComponent} from 'react'
import _ from 'lodash'

import ExpressionNode from 'src/ifql/components/ExpressionNode'
import VariableName from 'src/ifql/components/VariableName'
import FuncSelector from 'src/ifql/components/FuncSelector'

import {FlatBody, Suggestion} from 'src/types/ifql'

interface Props {
  body: Body[]
  suggestions: Suggestion[]
}

interface Body extends FlatBody {
  id: string
}

class BodyBuilder extends PureComponent<Props> {
  public render() {
    const bodybuilder = this.props.body.map(b => {
      if (b.declarations.length) {
        return b.declarations.map(d => {
          if (d.funcs) {
            return (
              <div className="declaration" key={b.id}>
                <VariableName name={d.name} />
                <ExpressionNode
                  key={b.id}
                  bodyID={b.id}
                  declarationID={d.id}
                  funcNames={this.funcNames}
                  funcs={d.funcs}
                />
              </div>
            )
          }

          return (
            <div className="declaration" key={b.id}>
              <VariableName name={b.source} />
            </div>
          )
        })
      }

      return (
        <div className="declaration" key={b.id}>
          <VariableName />
          <ExpressionNode
            bodyID={b.id}
            funcs={b.funcs}
            funcNames={this.funcNames}
          />
        </div>
      )
    })

    return (
      <div className="body-builder">
        {_.flatten(bodybuilder)}
        <div className="declaration">
          <FuncSelector
            bodyID="fake-body-id"
            declarationID="fake-declaration-id"
            onAddNode={this.createNewDeclaration}
            funcs={this.newDeclarationFuncs}
            connectorVisible={false}
          />
        </div>
      </div>
    )
  }

  private get newDeclarationFuncs(): string[] {
    // 'JOIN' only available if there are at least 2 named declarations
    return ['from', 'join', 'variable']
  }

  private createNewDeclaration = (bodyID, name, declarationID) => {
    // Returning a string here so linter stops yelling
    // TODO: write a real function

    return `${bodyID} / ${name} / ${declarationID}`
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default BodyBuilder
