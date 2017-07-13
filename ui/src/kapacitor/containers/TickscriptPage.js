import React, {PropTypes, Component} from 'react'
import Tickscript from 'src/kapacitor/components/Tickscript'

class TickscriptPage extends Component {
  constructor(props) {
    super(props)
  }

  handleSave() {
    console.log('save me!') // eslint-disable-line no-console
  }

  render() {
    const {source} = this.props

    return <Tickscript onSave={this.handleSave} source={source} />
  }
}

const {shape, string} = PropTypes

TickscriptPage.propTypes = {
  source: shape({
    name: string,
  }),
}

export default TickscriptPage
