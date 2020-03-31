// Libraries
import React, {SFC} from 'react'

// Components
import ToolbarFunction from 'src/timeMachine/components/fluxFunctionsToolbar/ToolbarFunction'

// Types
import {FluxToolbarFunction} from 'src/types/shared'

interface Props {
  category: string
  funcs: FluxToolbarFunction[]
  onClickFunction: (func: FluxToolbarFunction) => void
}

const FunctionCategory: SFC<Props> = props => {
  const {category, funcs, onClickFunction} = props

  return (
    <dl className="flux-toolbar--category">
      <dt className="flux-toolbar--heading">{category}</dt>
      {funcs.map(func => (
        <ToolbarFunction
          onClickFunction={onClickFunction}
          key={`${func.name}_${func.desc}`}
          func={func}
          testID={func.name}
        />
      ))}
    </dl>
  )
}

export default FunctionCategory
