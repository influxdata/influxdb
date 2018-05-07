import React, {PureComponent} from 'react'
import _ from 'lodash'

import ExpressionNode from 'src/ifql/components/ExpressionNode'
import VariableName from 'src/ifql/components/VariableName'

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

    return <div className="body-builder">{_.flatten(bodybuilder)}</div>
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default BodyBuilder
