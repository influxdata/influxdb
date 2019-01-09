import React from 'react'

export interface SigninProps {
  onSignInUser: () => void
}

export const AuthContext = React.createContext<SigninProps>(null)
