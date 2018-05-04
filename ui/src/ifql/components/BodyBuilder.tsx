import React, {PureComponent} from 'react'
import _ from 'lodash'

import ExpressionNode from 'src/ifql/components/ExpressionNode'

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
                <div className="variable-string">
                  <span className="variable-name">{d.name}</span>
                </div>
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
              <div className="variable-string">
                {this.colorVariableSyntax(b.source)}
              </div>
            </div>
          )
        })
      }

      return (
        <ExpressionNode
          key={b.id}
          bodyID={b.id}
          funcs={b.funcs}
          funcNames={this.funcNames}
        />
      )
    })

    return <div className="body-builder">{_.flatten(bodybuilder)}</div>
  }

  private colorVariableSyntax = (varString: string) => {
    const split = varString.split('=')
    const varName = split[0].substring(0, split[0].length - 1)
    const varValue = split[1].substring(1)

    const valueIsString = varValue.endsWith('"')

    return (
      <>
        <span className="variable-name">{varName}</span>
        {' = '}
        <span
          className={
            valueIsString ? 'variable-value--string' : 'variable-value--number'
          }
        >
          {varValue}
        </span>
      </>
    )
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default BodyBuilder
