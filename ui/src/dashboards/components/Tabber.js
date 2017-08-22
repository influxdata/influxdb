import React, {PropTypes} from 'react'

export const Tabber = ({labelText, children}) =>
  <div className="form-group col-sm-6">
    <label>
      {labelText}
    </label>
    <ul className="nav nav-tablist nav-tablist-sm">
      {children}
    </ul>
  </div>

export const Tab = ({isActive, onClickTab, text}) =>
  <li className={isActive ? 'active' : ''} onClick={onClickTab}>
    {text}
  </li>

const {bool, func, node, string} = PropTypes

Tabber.propTypes = {
  children: node.isRequired,
  labelText: string,
}

Tab.propTypes = {
  onClickTab: func.isRequired,
  isActive: bool.isRequired,
  text: string.isRequired,
}
