import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import MenuTooltipButton from 'src/shared/components/MenuTooltipButton'
import CustomTimeIndicator from 'src/shared/components/CustomTimeIndicator'
import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'
import {EDITING} from 'src/shared/annotations/helpers'
import {cellSupportsAnnotations} from 'src/shared/constants/index'

import {
  addingAnnotation,
  editingAnnotation,
  dismissEditingAnnotation,
} from 'src/shared/actions/annotations'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class LayoutCellMenu extends Component {
  state = {
    subMenuIsOpen: false,
  }

  handleToggleSubMenu = () => {
    this.setState({subMenuIsOpen: !this.state.subMenuIsOpen})
  }

  render() {
    const {subMenuIsOpen} = this.state
    const {
      mode,
      cell,
      onEdit,
      onClone,
      queries,
      onDelete,
      isEditable,
      dataExists,
      onCSVDownload,
      onStartAddingAnnotation,
      onStartEditingAnnotation,
      onDismissEditingAnnotation,
    } = this.props

    const menuOptions = [
      {
        text: 'Configure',
        action: onEdit(cell),
      },
      {
        text: 'Add Annotation',
        action: onStartAddingAnnotation,
        disabled: !cellSupportsAnnotations(cell.type),
      },
      {
        text: 'Edit Annotations',
        action: onStartEditingAnnotation,
        disabled: !cellSupportsAnnotations(cell.type),
      },
      {
        text: 'Download CSV',
        action: onCSVDownload(cell),
        disabled: !dataExists,
      },
    ]

    return (
      <div
        className={classnames('dash-graph-context', {
          'dash-graph-context__open': subMenuIsOpen,
        })}
      >
        <div
          className={`${
            isEditable
              ? 'dash-graph--custom-indicators dash-graph--draggable'
              : 'dash-graph--custom-indicators'
          }`}
        >
          {queries && <CustomTimeIndicator queries={queries} />}
        </div>
        {isEditable &&
          mode !== EDITING && (
            <div className="dash-graph-context--buttons">
              {queries.length ? (
                <MenuTooltipButton
                  icon="pencil"
                  menuOptions={menuOptions}
                  informParent={this.handleToggleSubMenu}
                />
              ) : null}
              <Authorized requiredRole={EDITOR_ROLE}>
                <MenuTooltipButton
                  icon="duplicate"
                  menuOptions={[{text: 'Clone Cell', action: onClone(cell)}]}
                  informParent={this.handleToggleSubMenu}
                />
              </Authorized>
              <MenuTooltipButton
                icon="trash"
                theme="danger"
                menuOptions={[{text: 'Confirm', action: onDelete(cell)}]}
                informParent={this.handleToggleSubMenu}
              />
            </div>
          )}
        {mode === 'editing' &&
          cellSupportsAnnotations(cell.type) && (
            <div className="dash-graph-context--buttons">
              <div
                className="btn btn-xs btn-success"
                onClick={onDismissEditingAnnotation}
              >
                Done Editing
              </div>
            </div>
          )}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

LayoutCellMenu.propTypes = {
  mode: string,
  onEdit: func,
  onClone: func,
  onDelete: func,
  cell: shape(),
  isEditable: bool,
  dataExists: bool,
  onCSVDownload: func,
  queries: arrayOf(shape()),
  onStartAddingAnnotation: func.isRequired,
  onStartEditingAnnotation: func.isRequired,
  onDismissEditingAnnotation: func.isRequired,
}

const mapStateToProps = ({annotations: {mode}}) => ({
  mode,
})

const mapDispatchToProps = dispatch => ({
  onStartAddingAnnotation: bindActionCreators(addingAnnotation, dispatch),
  onStartEditingAnnotation: bindActionCreators(editingAnnotation, dispatch),
  onDismissEditingAnnotation: bindActionCreators(
    dismissEditingAnnotation,
    dispatch
  ),
})

export default connect(mapStateToProps, mapDispatchToProps)(LayoutCellMenu)
