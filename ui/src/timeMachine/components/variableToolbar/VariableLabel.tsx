import React, {FunctionComponent} from 'react'

interface Props {
  name: string
  onClickVariable: (name: string) => void
}

const VariableLabel: FunctionComponent<Props> = ({onClickVariable, name}) => {
  return (
    <div
      className="variables-toolbar--label"
      onClick={() => {
        onClickVariable(name)
      }}
    >
      {name}
    </div>
  )
}

export default VariableLabel
