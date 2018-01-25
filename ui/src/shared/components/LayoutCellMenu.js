import React, {Component, PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import classnames from 'classnames'

import MenuTooltipButton from 'src/shared/components/MenuTooltipButton'
import CustomTimeIndicator from 'src/shared/components/CustomTimeIndicator'

import {EDITING} from 'src/shared/annotations/helpers'

import {
  addingAnnotation,
  editingAnnotation,
  dismissEditingAnnotation,
} from 'src/shared/actions/annotations'

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
      queries,
      onDelete,
      isEditable,
      dataExists,
      onCSVDownload,
      onStartAddingAnnotation,
      onStartEditingAnnotation,
      onDismissEditingAnnotation,
    } = this.props

    return (
      <div
        className={classnames('dash-graph-context', {
          'dash-graph-context__open': subMenuIsOpen,
        })}
      >
        <div
          className={`${isEditable
            ? 'dash-graph--custom-indicators dash-graph--draggable'
            : 'dash-graph--custom-indicators'}`}
        >
          {queries && <CustomTimeIndicator queries={queries} />}
        </div>
        {isEditable &&
          mode !== EDITING &&
          <div className="dash-graph-context--buttons">
            {queries.length
              ? <MenuTooltipButton
                  icon="pencil"
                  menuOptions={[
                    {text: 'Configure', action: onEdit(cell)},
                    {text: 'Add Annotation', action: onStartAddingAnnotation},
                    {
                      text: 'Edit Annotations',
                      action: onStartEditingAnnotation,
                    },
                  ]}
                  informParent={this.handleToggleSubMenu}
                />
              : null}
            {dataExists &&
              <MenuTooltipButton
                icon="download"
                menuOptions={[
                  {text: 'Download CSV', action: onCSVDownload(cell)},
                ]}
                informParent={this.handleToggleSubMenu}
              />}
            <MenuTooltipButton
              icon="trash"
              theme="danger"
              menuOptions={[{text: 'Confirm', action: onDelete(cell)}]}
              informParent={this.handleToggleSubMenu}
            />
          </div>}
        {mode === 'editing' &&
          <div className="dash-graph-context--buttons">
            <div
              className="btn btn-xs btn-success"
              onClick={onDismissEditingAnnotation}
            >
              Done
            </div>
          </div>}
      </div>
    )
  }
}

const {arrayOf, bool, func, shape, string} = PropTypes

LayoutCellMenu.propTypes = {
  mode: string,
  onEdit: func,
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
