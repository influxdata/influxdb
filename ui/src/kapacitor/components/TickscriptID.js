import React, {PropTypes, Component} from 'react'

class TickscriptID extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {onChangeID, id} = this.props

    return (
      <input
        className="page-header--editing kapacitor-theme"
        autoFocus={true}
        value={id}
        onChange={onChangeID}
        placeholder="Name your tickscript"
        spellCheck={false}
        autoComplete={false}
      />
    )
  }
}

export const TickscriptStaticID = ({id}) =>
  <h1
    className="page-header__title page-header kapacitor-theme"
    style={{display: 'flex', justifyContent: 'baseline'}}
  >
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
