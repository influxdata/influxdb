import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import MenuTooltipButton from 'src/shared/components/MenuTooltipButton'
import CustomTimeIndicator from 'src/shared/components/CustomTimeIndicator'

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
      cell,
      onEdit,
      queries,
      onDelete,
      isEditable,
      dataExists,
      onCSVDownload,
      onStartAddAnnotation,
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
          <div className="dash-graph-context--buttons">
            <MenuTooltipButton
              icon="pencil"
              menuOptions={[
                {text: 'Queries', action: onEdit(cell)},
                {text: 'Add Annotation', action: onStartAddAnnotation},
              ]}
              informParent={this.handleToggleSubMenu}
            />
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
      </div>
    )
  }
}

const {arrayOf, bool, func, shape} = PropTypes

LayoutCellMenu.propTypes = {
  onEdit: func,
  onDelete: func,
  cell: shape(),
  isEditable: bool,
  dataExists: bool,
  onCSVDownload: func,
  queries: arrayOf(shape()),
  onStartAddAnnotation: func.isRequired,
}

export default LayoutCellMenu
