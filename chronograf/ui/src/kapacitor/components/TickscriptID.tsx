import React, {Component, SFC, ChangeEvent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface TickscriptIDProps {
  onChangeID: (e: ChangeEvent<HTMLInputElement>) => void
  id: string
}

@ErrorHandling
class TickscriptID extends Component<TickscriptIDProps> {
  constructor(props) {
    super(props)
  }

  public render() {
    const {onChangeID, id} = this.props

    return (
      <input
        className="form-control input-sm form-malachite"
        autoFocus={true}
        value={id}
        onChange={onChangeID}
        placeholder="ID your TICKscript"
        spellCheck={false}
        autoComplete="off"
      />
    )
  }
}

interface TickscriptStaticIDProps {
  id: string
}
export const TickscriptStaticID: SFC<TickscriptStaticIDProps> = ({id}) => (
  <h1 className="tickscript-controls--name">{id}</h1>
)

export default TickscriptID
