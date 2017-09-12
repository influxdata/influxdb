import React, {PropTypes, Component} from 'react'

class TickscriptName extends Component {
  constructor(props) {
    super(props)
  }

  render() {
    const {onChangeName, name} = this.props

    return (
      <input
        className="page-header--editing kapacitor-theme"
        autoFocus={true}
        value={name}
        onChange={onChangeName}
        placeholder="Name your tickscript"
        spellCheck={false}
        autoComplete={false}
      />
    )
  }
}

export const TickscriptStaticName = ({name}) =>
  <h1
    className="page-header__title page-header kapacitor-theme"
    style={{display: 'flex', justifyContent: 'baseline'}}
  >
    {name}
  </h1>

const {func, string} = PropTypes

TickscriptName.propTypes = {
  onChangeName: func.isRequired,
  name: string.isRequired,
}

TickscriptStaticName.propTypes = {
  name: string.isRequired,
}

export default TickscriptName
