import React from 'react'
import {routerActions} from 'react-router-redux'
import {UserAuthWrapper} from 'redux-auth-wrapper'

export const UserIsAuthenticated = UserAuthWrapper({
  authSelector: ({auth}) => ({auth}),
  authenticatingSelector: ({auth: {isAuthLoading}}) => isAuthLoading,
  LoadingComponent: (() => <div className="page-spinner" />),
  redirectAction: routerActions.replace,
  wrapperDisplayName: 'UserIsAuthenticated',
  predicate: ({auth: {me, links, isAuthLoading}}) => isAuthLoading === false && links.length && me !== null,
})
//
// UserIsAuthenticated.onEnter = (store, nextState, replace) => {
//   debugger
//   console.log('bob')
// }

export const Authenticated = UserIsAuthenticated((props) => React.cloneElement(props.children, props))

export const UserIsNotAuthenticated = UserAuthWrapper({
  authSelector: ({auth}) => ({auth}),
  authenticatingSelector: ({auth: {isAuthLoading}}) => isAuthLoading,
  LoadingComponent: (() => <div className="page-spinner" />),
  redirectAction: routerActions.replace,
  wrapperDisplayName: 'UserIsNotAuthenticated',
  predicate: ({auth: {me, links, isAuthLoading}}) => isAuthLoading === false && links.length > 0 && me === null,
  failureRedirectPath: () => '/',
  allowRedirectBack: false,
})
