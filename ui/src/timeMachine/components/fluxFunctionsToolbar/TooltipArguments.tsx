import React, {PureComponent} from 'react'

interface Args {
  name: string
  type: string
  desc: string
}

interface Props {
  argsList?: Args[]
}

class TooltipArguments extends PureComponent<Props> {
  public render() {
    return (
      <article>
        <div className="flux-function-docs--heading">Arguments</div>
        <div className="flux-function-docs--snippet">{this.arguments}</div>
      </article>
    )
  }

  private get arguments(): JSX.Element | JSX.Element[] {
    const {argsList} = this.props

    if (argsList.length > 0) {
      return argsList.map(a => {
        return (
          <div className="flux-function-docs--arguments" key={a.name}>
            <span>{a.name}:</span>
            <span>{a.type}</span>
            <div>{a.desc}</div>
          </div>
        )
      })
    }

    return <div className="flux-function-docs--arguments">None</div>
  }
}

export default TooltipArguments
