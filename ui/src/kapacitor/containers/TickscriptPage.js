import React, {PropTypes, Component} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import Tickscript from 'src/kapacitor/components/Tickscript'
import * as kapactiorActionCreators from 'src/kapacitor/actions/view'
import * as errorActionCreators from 'shared/actions/errors'
import {getActiveKapacitor} from 'src/shared/apis'

class TickscriptPage extends Component {
  constructor(props) {
    super(props)

    this.state = {
      kapacitor: {},
      task: {
        id: '',
        name: '',
        status: 'enabled',
        tickscript: '',
        dbrps: [],
        type: 'stream',
      },
      validation: '',
      isEditingID: true,
      areLogsVisible: true,
    }
  }

  async componentDidMount() {
    const {
      source,
      errorActions,
      kapacitorActions,
      params: {ruleID},
    } = this.props

    const kapacitor = await getActiveKapacitor(source)
    if (!kapacitor) {
      errorActions.errorThrown(
        'We could not find a configured Kapacitor for this source'
      )
    }

    if (this._isEditing()) {
      await kapacitorActions.getRule(kapacitor, ruleID)
      const {id, name, tickscript, dbrps, type} = this.props.rules.find(
        r => r.id === ruleID
      )

      this.setState({task: {tickscript, dbrps, type, status, name, id}})
    }

    this.setState({kapacitor})
  }

  handleSave = async () => {
    const {kapacitor, task} = this.state
    const {
      source: {id: sourceID},
      router,
      kapacitorActions: {createTask, updateTask},
      params: {ruleID},
    } = this.props

    let response

    try {
      if (this._isEditing()) {
        response = await updateTask(kapacitor, task, ruleID, router, sourceID)
      } else {
        response = await createTask(kapacitor, task, router, sourceID)
      }

      if (response && response.code === 500) {
        return this.setState({validation: response.message})
      }
    } catch (error) {
      console.error(error)
      throw error
    }
  }

  handleChangeScript = tickscript => {
    this.setState({task: {...this.state.task, tickscript}})
  }

  handleSelectDbrps = dbrps => {
    this.setState({task: {...this.state.task, dbrps}})
  }

  handleChangeType = type => () => {
    this.setState({task: {...this.state.task, type}})
  }

  handleChangeID = e => {
    this.setState({task: {...this.state.task, id: e.target.value}})
  }

  HandleToggleLogsVisbility = () => {
    this.setState({areLogsVisible: !this.state.areLogsVisible})
  }

  render() {
    const {source} = this.props
    const {task, validation, areLogsVisible} = this.state

    return (
      <Tickscript
        task={task}
        source={source}
        validation={validation}
        onSave={this.handleSave}
        isNewTickscript={!this._isEditing()}
        onSelectDbrps={this.handleSelectDbrps}
        onChangeScript={this.handleChangeScript}
        onChangeType={this.handleChangeType}
        onChangeID={this.handleChangeID}
        areLogsVisible={areLogsVisible}
        onToggleLogsVisbility={this.HandleToggleLogsVisbility}
      />
    )
  }

  _isEditing() {
    const {params} = this.props
    return params.ruleID && params.ruleID !== 'new'
  }
}

const {arrayOf, func, shape, string} = PropTypes

TickscriptPage.propTypes = {
  source: shape({
    name: string,
  }),
  errorActions: shape({
    errorThrown: func.isRequired,
  }).isRequired,
  kapacitorActions: shape({
    updateTask: func.isRequired,
    createTask: func.isRequired,
    getRule: func.isRequired,
  }),
  router: shape({
    push: func.isRequired,
  }).isRequired,
  params: shape({
    ruleID: string,
  }).isRequired,
  rules: arrayOf(shape()),
}

const mapStateToProps = state => {
  return {
    rules: Object.values(state.rules),
  }
}

const mapDispatchToProps = dispatch => ({
  kapacitorActions: bindActionCreators(kapactiorActionCreators, dispatch),
  errorActions: bindActionCreators(errorActionCreators, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(TickscriptPage)
