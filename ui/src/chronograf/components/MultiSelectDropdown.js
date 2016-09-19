import React, {PropTypes} from 'react';
import OnClickOutside from 'shared/components/OnClickOutside';
import classNames from 'classnames';
import _ from 'lodash';

const {func, arrayOf, string} = PropTypes;
const MultiSelectDropdown = React.createClass({
  propTypes: {
    onApply: func.isRequired,
    items: arrayOf(PropTypes.string.isRequired).isRequired,
    selectedItems: arrayOf(string.isRequired).isRequired,
  },

  getInitialState() {
    return {
      isOpen: false,
      localSelectedItems: this.props.selectedItems,
    };
  },

  componentWillReceiveProps(nextProps) {
    if (!_.isEqual(this.state.localSelectedItems, nextProps.selectedItems)) {
      this.setState({
        localSelectedItems: nextProps.selectedItems,
      });
    }
  },

  handleClickOutside() {
    this.setState({isOpen: false});
  },

  toggleMenu(e) {
    e.stopPropagation();
    this.setState({isOpen: !this.state.isOpen});
  },

  onSelect(item, e) {
    e.stopPropagation();

    const {localSelectedItems} = this.state;

    let nextItems;
    if (this.isSelected(item)) {
      nextItems = localSelectedItems.filter((i) => i !== item);
    } else {
      nextItems = localSelectedItems.concat(item);
    }

    this.setState({localSelectedItems: nextItems});
  },

  isSelected(item) {
    return this.state.localSelectedItems.indexOf(item) > -1;
  },

  onApplyFunctions(e) {
    e.stopPropagation();

    this.setState({isOpen: false});
    this.props.onApply(this.state.localSelectedItems);
  },

  render() {
    const {localSelectedItems} = this.state;

    return (
      <div className="dropdown multi-select-dropdown">
        <div onClick={this.toggleMenu} className="btn btn-xs btn-info dropdown-toggle multi-select-dropdown__toggle" type="button">
          <span className="multi-select-dropdown__label">
            {
              localSelectedItems.length ? localSelectedItems.map((s) => s).join(', ') : 'Apply Function'
            }
          </span>
          <span className="caret multi-select-dropdown__caret"></span>
        </div>
        {this.renderMenu()}
      </div>
    );
  },

  renderMenu() {
    const {items} = this.props;
    const {isOpen} = this.state;

    if (!isOpen) {
      return null;
    }

    return (
      <div className="dropdown-options">
        <ul className="dropdown-menu multi-select-dropdown__menu" aria-labelledby="dropdownMenu1">
          {items.map((listItem, i) => {
            return (
              <li className={classNames('multi-select-dropdown__item', {active: this.isSelected(listItem)})} key={i} onClick={_.wrap(listItem, this.onSelect)}>
                <a href="#">{listItem}</a>
              </li>
            );
          })}
          <li className="multi-select-dropdown__apply" onClick={this.onApplyFunctions}>
            <div className="btn btn-sm btn-primary btn-block">Apply</div>
          </li>
        </ul>
      </div>
    );
  },
});

export default OnClickOutside(MultiSelectDropdown);
