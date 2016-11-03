import React, {PropTypes} from 'react';
import OnClickOutside from 'shared/components/OnClickOutside';

const Dropdown = React.createClass({
  propTypes: {
    items: PropTypes.arrayOf(PropTypes.shape({
      text: PropTypes.string.isRequired,
    })).isRequired,
    onChoose: PropTypes.func.isRequired,
    selected: PropTypes.string.isRequired,
    className: PropTypes.string,
  },
  getInitialState() {
    return {
      isOpen: false,
    };
  },
  getDefaultProps() {
    return {
      actions: [],
    };
  },
  handleClickOutside() {
    this.setState({isOpen: false});
  },
  handleSelection(item) {
    this.toggleMenu();
    this.props.onChoose(item);
  },
  toggleMenu(e) {
    if (e) {
      e.stopPropagation();
    }
    this.setState({isOpen: !this.state.isOpen});
  },
  handleAction(e, action, item) {
    e.stopPropagation();
    action.handler(item);
  },
  render() {
    const self = this;
    const {items, selected, className, actions} = self.props;

    return (
      <div onClick={this.toggleMenu} className={`dropdown ${className}`}>
        <div className="btn btn-sm btn-info dropdown-toggle">
          <span className="dropdown-selected">{selected}</span>
          <span className="caret" />
        </div>
        {self.state.isOpen ?
          <ul className="dropdown-menu show">
            {items.map((item, i) => {
              return (
                <li className={`dropdown ${className}__item`} key={i}>
                  <a href="#" onClick={() => self.handleSelection(item)}>
                    {item.text}
                  </a>
                  <div className="dropdown-item__actions">
                    {actions.map((action) => {
                      return (
                        <button key={action.text} data-target={action.target} data-toggle="modal" className="dropdown-item__action" onClick={(e) => self.handleAction(e, action, item)}>
                          <span title={action.text} className={`icon ${action.icon}`}></span>
                        </button>
                      );
                    })}
                  </div>
                </li>
              );
            })}
          </ul>
          : null}
      </div>
    );
  },
});

export default OnClickOutside(Dropdown);
