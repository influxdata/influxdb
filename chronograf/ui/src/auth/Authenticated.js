import React from 'react'
import {replace} from 'react-router-redux'
import {UserAuthWrapper} from 'redux-auth-wrapper'

export const UserIsAuthenticated = UserAuthWrapper({
  authSelector: ({auth}) => ({auth}),
  authenticatingSelector: ({auth: {isMeLoading}}) => isMeLoading,
  LoadingComponent: () => <div className="page-spinner" />,
  redirectAction: replace,
  wrapperDisplayName: 'UserIsAuthenticated',
  predicate: ({auth: {me, isMeLoading}}) => !isMeLoading && me !== null,
})

export const UserIsNotAuthenticated = UserAuthWrapper({
  authSelector: ({auth}) => ({auth}),
  authenticatingSelector: ({auth: {isMeLoading}}) => isMeLoading,
  LoadingComponent: () => <div className="page-spinner" />,
  redirectAction: replace,
  wrapperDisplayName: 'UserIsNotAuthenticated',
  predicate: ({auth: {me, isMeLoading}}) => !isMeLoading && me === null,
  failureRedirectPath: () => '/',
  allowRedirectBack: false,
})
