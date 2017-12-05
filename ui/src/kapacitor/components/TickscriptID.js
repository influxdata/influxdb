import React, {PropTypes, Component} from 'react'

class TickscriptID extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {onChangeID, id} = this.props

    return (
      <input
        className="form-control input-sm form-malachite"
        autoFocus={true}
        value={id}
        onChange={onChangeID}
        placeholder="ID your TICKscript"
        spellCheck={false}
        autoComplete={false}
      />
    )
  }
}

export const TickscriptStaticID = ({id}) =>
  <h1 className="tickscript-controls--name">
    {id}
  </h1>

const {func, string} = PropTypes

TickscriptID.propTypes = {
  onChangeID: func.isRequired,
  id: string.isRequired,
}

TickscriptStaticID.propTypes = {
  id: string.isRequired,
}

export default TickscriptID
