import React from 'react'
import {routerActions} from 'react-router-redux'
import {UserAuthWrapper} from 'redux-auth-wrapper'

export const UserIsAuthenticated = UserAuthWrapper({
  authSelector: ({auth}) => ({auth}),
  authenticatingSelector: ({auth: {isMeLoading}}) => isMeLoading,
  LoadingComponent: (() => <div className="page-spinner" />),
  redirectAction: routerActions.replace,
  wrapperDisplayName: 'UserIsAuthenticated',
  predicate: ({auth: {me, isMeLoading}}) => !isMeLoading && me !== null,
})
//
// UserIsAuthenticated.onEnter = (store, nextState, replace) => {
//   debugger
//   console.log('bob')
// }

export const Authenticated = UserIsAuthenticated((props) => React.cloneElement(props.children, props))

export const UserIsNotAuthenticated = UserAuthWrapper({
  authSelector: ({auth}) => ({auth}),
  authenticatingSelector: ({auth: {isMeLoading}}) => isMeLoading,
  LoadingComponent: (() => <div className="page-spinner" />),
  redirectAction: routerActions.replace,
  wrapperDisplayName: 'UserIsNotAuthenticated',
  predicate: ({auth: {me, isMeLoading}}) => !isMeLoading && me === null,
  failureRedirectPath: () => '/',
  allowRedirectBack: false,
})
