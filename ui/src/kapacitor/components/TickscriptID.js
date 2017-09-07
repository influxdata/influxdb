import React, {PropTypes, Component} from 'react'
import ReactTooltip from 'react-tooltip'
import onClickOutside from 'react-onclickoutside'

class TickscriptNewID extends Component {
  constructor(props) {
    super(props)
    this.state = {
      tickscriptID: '',
    }
  }

  handleChangeID = e => {
    const tickscriptID = e.target.value
    this.setState({tickscriptID})
  }

  handleClickOutside() {
    this.props.onStopEdit()
  }

  render() {
    const {isEditing, onStartEdit} = this.props
    const {tickscriptID} = this.state

    return isEditing
      ? <input
          className="page-header--editing kapacitor-theme"
          autoFocus={true}
          value={tickscriptID}
          onChange={this.handleChangeID}
          placeholder="Name your tickscript"
          spellCheck={false}
          autoComplete={false}
        />
      : <h1
          className="page-header__title page-header--editable kapacitor-theme"
          onClick={onStartEdit}
          data-for="rename-kapacitor-tooltip"
          data-tip="Click to Rename"
        >
          {tickscriptID}
          <ReactTooltip
            id="rename-kapacitor-tooltip"
            delayShow={200}
            effect="solid"
            html={true}
            offset={{top: 2}}
            place="bottom"
            class="influx-tooltip kapacitor-tooltip place-bottom"
          />
        </h1>
  }
}

export const TickscriptEditID = ({id}) =>
  <h1 className="page-header__title page-header kapacitor-theme">
    {id}
  </h1>

const {bool, func, string} = PropTypes

TickscriptNewID.propTypes = {
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  onStopEdit: func.isRequired,
  isNewTickscript: bool.isRequired,
}

TickscriptEditID.propTypes = {
  id: string.isRequired,
}

export default onClickOutside(TickscriptNewID)
