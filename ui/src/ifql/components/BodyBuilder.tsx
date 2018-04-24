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
              <ExpressionNode
                id={d.id}
                key={d.id}
                funcNames={this.funcNames}
                funcs={d.funcs}
              />
            )
          }

          return <div key={b.id}>{b.source}</div>
        })
      }

      return (
        <ExpressionNode
          id={b.id}
          key={b.id}
          funcNames={this.funcNames}
          funcs={b.funcs}
        />
      )
    })

    return _.flatten(bodybuilder)
  }

  private get funcNames() {
    return this.props.suggestions.map(f => f.name)
  }
}

export default BodyBuilder
